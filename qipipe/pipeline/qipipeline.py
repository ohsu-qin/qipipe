import os, tempfile
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Merge)
from ..helpers import xnat_helper
from .workflow_base import WorkflowBase
from . import staging
from .staging import StagingWorkflow
from .mask import MaskWorkflow
from .registration import RegistrationWorkflow
from .modeling import ModelingWorkflow

import logging
logger = logging.getLogger(__name__)


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
    and runs it on the given inputs.
    
    :param inputs: the :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        inputs
    :param opts: the :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
        initializer and :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        options
    :return: the :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        result
    """
    wf_gen = QIPipelineWorkflow(**opts)
    # The base directory is used solely for workflow creation.
    opts.pop('base_dir', None)
    return wf_gen.run(*inputs, **opts)


class QIPipelineWorkflow(WorkflowBase):
    """
    QIPipeline builds and executes the OHSU QIN workflows.
    The pipeline builds a composite workflow which stitches together
    the following constituent workflows:
    
    - staging: Prepare the new AIRC DICOM visits, as described in
      :class:`qipipe.staging.StagingWorkflow`
    
    - mask: Create a mask from the staged images,
      as described in :class:`qipipe.staging.MaskWorkflow`
    
    - registration: Mask, register and realign the staged images,
      as described in :class:`qipipe.staging.RegistrationWorkflow`
    
    - modeling: Perform PK modeling as described in
      :class:`qipipe.staging.ModelingWorkflow`
    
    The easiest way to execute the pipeline is to call the
    :meth:`qipipe.pipeline.qipipeline.run` method.
    
    The pipeline execution workflow is also available as the
    `workflow` instance variable. The workflow input is a node
    named ``input_spec`` with the same fields as the staging `input_spec`.
    
    In addition, there are two iterable inputs:
    
    - The `iter_session` node input field `session_spec` must be set
      to the (subject, session) input tuples
    
    - The `iter_series` node input field `series_spec` must be set
      to the (subject, session, scan, dicom_files) input tuples
    """
    
    def __init__(self, **opts):
        """
        Builds the pipeline workflow.
        
        The default workflow settings can be overriden by a configuration
        file specified in the `staging`, `mask`, `registration` or
        `modeling` option. If the `mask` option is set to False, then
        only staging is performed. If the `registration` option is set
        to false, then  registration is skipped and modeling is performed on
        the staged scans. If the `modeling` option is set to False, then
        PK modeling is not performed.
        
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword staging: the optional staging configuration file
        :keyword mask: the optional mask configuration file, or False to
            halt after staging the files
        :keyword registration: the optional registration configuration file,
            or False to skip registration and model the staged scans
        :keyword modeling: the optional modeling configuration file, or
            False to skip modeling
        """
        super(QIPipelineWorkflow, self).__init__(logger)
        
        self.workflow = self._create_workflow(**opts)
        """
        The pipeline execution workflow. The execution workflow is executed by
        calling the :meth:`qipipe.pipeline.modeling.QIPipelineWorkflow.run`
        method.
        """
        
        self.registration_reconstruction = None
        """The registration XNAT reconstruction name."""
        
        self.modeling_assessor = None
        """The modeling XNAT assessor name."""
    
    def run(self, collection, *inputs, **opts):
        """
        Runs the OHSU QIN pipeline on the the given AIRC subject directories.
        The supported AIRC collections are listed in
        :mod:`qipipe.staging.airc_collection`.
        
        This method returns a
        {subject: {session: results}} dictionary
        for the new staged subject and sessions, where results is
        a dictionary with the following items:
            
        - ``registration``: the registration XNAT reconstruction name
        
        - ``modeling``: the modeling XNAT assessor name
        
        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories to stage
        :param opts: the meth:`qipipe.pipeline.staging.run` options,
            modified as follows:
        :keyword dest: the TCIA upload destination directory
            (default is subdirectory named ``staged`` in the
            current working directory)
        :return: the new {subject: session: results}  dictionary
        """
        # The staging location.
        if opts.has_key('dest'):
            dest = os.path.abspath(opts.pop('dest'))
        else:
            dest = os.path.join(os.getcwd(), 'staged')
        
        
        # Delegate to staging with the executive workflow. Staging
        # executes the workflow on the new inputs.
        stg_dict = staging.run(collection, *inputs, dest=dest,
            base_dir=self.workflow.base_dir, workflow=self.workflow, **opts)
        
        # Return the new {subject: session: results}  dictionary,
        # where results includes the session scans, the registration
        # XNAT reconstruction name and the modeling XNAT assessor
        # name.
        results = {}
        if self.registration_reconstruction:
            results['registration'] = self.registration_reconstruction
        if self.modeling_assessor:
            results['modeling'] = self.modeling_assessor
        output_dict = defaultdict(lambda: defaultdict(dict))
        for sbj, sess_dict in stg_dict.iteritems():
            for sess, scans in sess_dict.iteritems():
                sess_results = results.copy()
                sess_results['scans'] = scans
                output_dict[sbj][sess] = sess_results
        return output_dict
    
    def _create_workflow(self, **opts):
        """
        Builds the reusable pipeline workflow described in
        :class:`qipipe.pipeline.qipipeline.QIPipeline`.
        
        :param opts: the constituent workflow initializer options
        :return: the Nipype workflow
        """
        logger.debug("Building the QIN pipeline execution workflow...")
        
        # The work directory used for the master workflow and all
        # constituent workflows.
        base_dir_opt = opts.get('base_dir', None)
        if base_dir_opt:
            base_dir = os.path.abspath(base_dir_opt)
        else:
            base_dir = tempfile.mkdtemp()
        
        # The execution workflow.
        exec_wf = pe.Workflow(name='qin_exec', base_dir=base_dir)
        
        # The staging workflow.
        stg_opt = opts.get('staging', None)
        if stg_opt == False:
            stg_wf = None
        else:
            stg_cfg = opts.get('staging', None)
            stg_opts = dict(cfg_file=stg_cfg, base_dir=base_dir)
            stg_wf = StagingWorkflow(**stg_opts).workflow
        
        # The mask workflow.
        mask_opt = opts.get('mask', None)
        if mask_opt == False:
            mask_wf = None
            reg_opt = False
        else:
            mask_opts = dict(cfg_file=mask_opt, base_dir=base_dir)
            mask_wf = MaskWorkflow(**mask_opts).workflow
            reg_opt = opts.get('registration', None)
        
        # The registration workflow.
        if reg_opt == False:
            reg_wf = None
            mdl_opt = False
        else:
            reg_opts = dict(cfg_file=reg_opt, base_dir=base_dir)
            reg_wf_gen = RegistrationWorkflow(**reg_opts)
            reg_wf = reg_wf_gen.workflow
            self.registration_reconstruction = reg_wf_gen.reconstruction
            mdl_opt = opts.get('modeling', None)
        
        # The modeling workflow.
        if mdl_opt == False:
            mdl_wf = None
        else:
            mdl_opts = dict(cfg_file=mdl_opt, base_dir=base_dir)
            mdl_wf_gen = ModelingWorkflow(**mdl_opts)
            mdl_wf = mdl_wf_gen.workflow
            self.modeling_assessor = mdl_wf_gen.assessor
        
        # The workflow inputs.
        in_fields = ['subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
            name='input_spec')
        
        # Stitch together the workflows.
        exec_wf.connect(input_spec, 'subject', stg_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', stg_wf, 'input_spec.session')
        
        # Create the mask.
        if mask_wf:
            exec_wf.connect(input_spec, 'subject', mask_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session', mask_wf, 'input_spec.session')
            exec_wf.connect(stg_wf, 'output_spec.images', mask_wf, 'input_spec.images')
        
        # Register the staged images.
        if reg_wf:
            exec_wf.connect(input_spec, 'subject', reg_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session', reg_wf, 'input_spec.session')
            exec_wf.connect(mask_wf, 'output_spec.mask', reg_wf, 'input_spec.mask')
            exec_wf.connect(stg_wf, 'output_spec.images', reg_wf, 'input_spec.images')
            exec_wf.connect(stg_wf, 'stack.out_file', reg_wf, 'iter_image.image')
        
        # Model the realigned images.
        if mdl_wf:
            exec_wf.connect(input_spec, 'subject', mdl_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session', mdl_wf, 'input_spec.session')
            exec_wf.connect(mask_wf, 'output_spec.mask', mdl_wf, 'input_spec.mask')
            exec_wf.connect(reg_wf, 'output_spec.images', mdl_wf, 'input_spec.images')
        
        logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(exec_wf)
        
        return exec_wf
    
    def _generate_child_workflow(self, klass, **opts):
        """
        :param klass: the workflow wrapper class
        :param opts: the workflow creation options described in the workflow
            wrapper class
        :return: the new child workflow instance
        """
        return klass(**opts).workflow
