import os, tempfile
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Merge)
from ..helpers import xnat_helper
from .workflow_base import WorkflowBase
from .staging import (detect_new_visits, StagingWorkflow)
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
        initializer options
    :return: the :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        result
    """
    # dest is a run option.
    run_opts = {}
    if 'dest' in opts:
        run_opts['dest'] = opts.pop('dest')
    return QIPipelineWorkflow(**opts).run(*inputs, **run_opts)


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
        
        mdl_anl = self.modeling_assessor = None
        """The modeling XNAT assessor name."""
    
    def run(self, collection, *inputs, **opts):
        """
        Runs the OHSU QIN pipeline on the the given AIRC subject directories.
        The supported AIRC collections are listed in
        :mod:`qipipe.staging.airc_collection`.
        
        This method returns a dictionary consisting of the following items:
        
        - `sessions`: the (subject, session) name tuples of the new sessions
          created in XNAT
        
        - `reconstruction`: the registration XNAT reconstruction name
        
        - `analysis`: the PK mapping XNAT analysis name
        
        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories to stage
        :param opts: the following workflow execution options:
        :keyword dest: the TCIA upload destination directory
            (default current working directory)
        :return: the (subject, session) XNAT names for the new sessions
        """
        # The AIRC series which are not yet uploaded to XNAT.
        new_series = detect_new_visits(collection, *inputs)
        if not new_series:
            return []
        
        # The {subject: session}, and {(subject, session): [(scan, dicom_files), ...]}
        # dictionaries.
        sbj_sess_dict = defaultdict(list)
        sess_ser_dict = defaultdict(list)
        for sbj, sess, scan, dicom_files in new_series:
            sbj_sess_dict[sbj].append((sbj, sess))
            sess_ser_dict[(sbj, sess)].append((scan, dicom_files))
        
        # The staging location.
        if opts.has_key('dest'):
            dest = os.path.abspath(opts['dest'])
        else:
            dest = os.path.join(os.getcwd(), 'data')

        # Set the workflow (collection, destination, subjects) input.
        subjects = sbj_sess_dict.keys()
        exec_wf = self.workflow
        input_spec = exec_wf.get_node('input_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.dest = dest
        input_spec.inputs.subjects = subjects
        
        # Set the iterable subject inputs.
        iter_subject = exec_wf.get_node('iter_subject')
        iter_subject.iterables = ('subject', subjects)
        
        # Set the iterable session inputs.
        iter_session = exec_wf.get_node('iter_session')
        iter_session.itersource = ('iter_subject', 'subject')
        iter_sess_fields = ['subject', 'session']
        iter_session.iterables = [iter_sess_fields, sbj_sess_dict]
        iter_session.synchronize = True
        
        # Set the iterable series inputs.
        iter_series = exec_wf.get_node('iter_series')
        iter_series.itersource = ('iter_session', ['subject', 'session'])
        iter_ser_fields = ['scan', 'dicom_files']
        iter_series.iterables = [iter_ser_fields, sess_ser_dict]
        iter_series.synchronize = True
        
        # Run the staging workflow.
        self._run_workflow(self.workflow)
        
        # Return the new XNAT (subject, session, reconstruction, analysis) tuples.
        reg_recon = self.registration_reconstruction
        mdl_anl = self.modeling_assessor
        return [(sbj, sess, reg_recon, mdl_anl) for sbj, sess in new_sessions]
    
    def _create_workflow(self, **opts):
        """
        Builds the reusable pipeline workflow described in
        :class:`qipipe.pipeline.qipipeline.QIPipeline`.
        
        :param opts: the constituent workflow initializer options
        :return: the Nipype workflow
        """
        logger.debug("Building the QIN pipeline execution workflow...")
        
        # The work directory used for the master workflow and all constituent
        # workflows.
        base_dir = opts.get('base_dir', None) or tempfile.mkdtemp()
        
        # The execution workflow.
        exec_wf = pe.Workflow(name='qin_exec', base_dir=base_dir)
        
        # The staging workflow.
        stg_opt = opts.get('staging', None)
        stg_wf_gen = self._workflow_generator(StagingWorkflow, base_dir, stg_opt)
        stg_wf = stg_wf_gen.workflow
        
        # The mask workflow.
        mask_opt = opts.get('mask', None)
        if mask_opt == False:
            logger.debug("Halting after staging since the mask option is set "
                "to False.")
            mask_wf = reg_wf = mdl_wf = None
        else:
            mask_wf_gen = self._workflow_generator(MaskWorkflow, base_dir, mask_opt)
            mask_wf = mask_wf_gen.workflow
            
            # The registration workflow.
            reg_opt = opts.get('registration', None)
            if reg_opt == False:
                logger.debug("Skipping registration since the registration "
                    "option is set to False.")
                reg_wf = None
            else:
                reg_wf_gen = self._workflow_generator(RegistrationWorkflow, base_dir,
                    reg_opt)
                reg_wf = reg_wf_gen.reusable_workflow
                self.registration_reconstruction = reg_wf_gen.reconstruction
            
            # The modeling workflow.
            mdl_opt = opts.get('modeling', None)
            if mdl_opt == False:
                logger.debug("Skipping modeling since the modeling option is "
                    "set to False.")
                mdl_wf = None
            else:
                mdl_wf_gen = self._workflow_generator(ModelingWorkflow, base_dir,
                    mdl_opt)
                mdl_wf = mdl_wf_gen.reusable_workflow
                self.modeling_assessor = mdl_wf_gen.assessor
        
        # The non-iterable workflow inputs.
        in_fields = ['collection', 'dest', 'subjects']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
            name='input_spec')
        logger.debug("The QIN pipeline non-iterable input is %s with "
            "fields %s" %(input_spec.name, in_fields))
        
        # The iterable subject input.
        iter_subject = pe.Node(IdentityInterface(fields=['subject']),
            name='iter_subject')
        logger.debug("The QIN pipeline iterable subject input is %s with fields %s" %
            (iter_subject.name, iter_subject.inputs.copyable_trait_names()))
        
        # The iterable session input.
        iter_session_fields = ['subject', 'session']
        iter_session = pe.Node(IdentityInterface(fields=iter_session_fields),
            name='iter_session')
        logger.debug("The QIN pipeline iterable session input is %s with fields %s" %
            (iter_session.name, iter_session.inputs.copyable_trait_names()))
        
        # The iterable series input.
        iter_series_fields = iter_session_fields + ['scan', 'dicom_files']
        iter_series = pe.Node(IdentityInterface(fields=iter_series_fields),
            name='iter_series')
        logger.debug("The QIN pipeline iterable series input is %s with fields %s" %
            (iter_series.name, iter_series.inputs.copyable_trait_names()))
        exec_wf.connect(iter_session, 'subject', iter_series, 'subject')
        exec_wf.connect(iter_session, 'session', iter_series, 'session')
        
        # Stitch together the workflows.
        tuple_func = lambda x: tuple(x)
        exec_wf.connect(input_spec, 'collection', stg_wf, 'input_spec.collection')
        exec_wf.connect(input_spec, 'dest', stg_wf, 'input_spec.dest')
        exec_wf.connect(input_spec, 'subjects', stg_wf, 'input_spec.subjects')
        exec_wf.connect(iter_subject, 'subject', stg_wf, 'iter_subject.subject')
        exec_wf.connect(iter_session, 'subject', stg_wf, 'iter_session.subject')
        exec_wf.connect(iter_session, 'session', stg_wf, 'iter_session.session')
        exec_wf.connect(iter_series, 'subject', stg_wf, 'iter_series.subject')
        exec_wf.connect(iter_series, 'session', stg_wf, 'iter_series.session')
        exec_wf.connect(iter_series, 'scan', stg_wf, 'iter_series.scan')
        exec_wf.connect(iter_series, 'dicom_files', stg_wf, 'iter_series.dicom_files')
        if mask_wf:
            # Collect the staged images.
            staged = pe.JoinNode(IdentityInterface(fields=['images']),
                joinsource='iter_series', name='staged')
            # Hook up the mask workflow inputs.
            exec_wf.connect(stg_wf, 'output_spec.out_file', staged, 'images')
            mask_inputs = pe.Node(Merge(2), name='mask_inputs')
            exec_wf.connect(iter_session, 'subject', mask_wf, 'input_spec.subject')
            exec_wf.connect(iter_session, 'session', mask_wf, 'input_spec.session')
            exec_wf.connect(staged, 'images', mask_wf, 'input_spec.images')
            if reg_wf:
                # Hook up the registration workflow inputs.
                exec_wf.connect(iter_session, 'subject', reg_wf, 'input_spec.subject')
                exec_wf.connect(iter_session, 'session', reg_wf, 'input_spec.session')
                exec_wf.connect(staged, 'images', reg_wf, 'input_spec.images')
                exec_wf.connect(mask_wf, 'output_spec.out_file', reg_wf, 'input_spec.mask')
                exec_wf.connect(stg_wf, 'output_spec.out_file', reg_wf, 'iter_image.image')
            if mdl_wf:
                # Hook up the modeling workflow non-iterable inputs.
                exec_wf.connect(iter_session, 'subject', mdl_wf, 'input_spec.subject')
                exec_wf.connect(iter_session, 'session', mdl_wf, 'input_spec.session')
                exec_wf.connect(mask_wf, 'output_spec.out_file', mdl_wf, 'input_spec.mask')
                if reg_wf:
                    # Model the realigned images.
                    realigned = pe.JoinNode(IdentityInterface(fields=['images']),
                        joinsource='iter_series', name='realigned')
                    exec_wf.connect(reg_wf, 'output_spec.out_file', realigned, 'images')
                    exec_wf.connect(realigned, 'images', mdl_wf, 'input_spec.images')
                else:
                    # Model the staged images.
                    exec_wf.connect(staged, 'images', mdl_wf, 'input_spec.images')
        
        logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(exec_wf)
        
        return exec_wf
    
    def _workflow_generator(self, factory, base_dir, cfg_file=None):
        """
        :param factory: the workflow class
        :param base_dir: the workflow base directory
        :param cfg_file: the workflow configuration file
        :return: the new workflow generator instance
        """
        opts = dict(base_dir=base_dir)
        if cfg_file:
            opts['cfg_file'] = cfg_file
        return factory(**opts)
