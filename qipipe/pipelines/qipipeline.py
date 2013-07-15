"""The qipipeline :meth:`run` function is the OHSU QIN pipeline facade."""

import os, tempfile
from ..helpers import xnat_helper
from .pipeline_error import PipelineError
from .staging import detect_new_visits, StagingWorkflow
from .registration import RegistrationWorkflow
from .modeling import ModelingWorkflow

import logging
logger = logging.getLogger(__name__)


def run(*inputs, **opts):
    """
    Creates a :class:`QIPipelineWorkflow` and runs its on the given inputs.
    
    :param inputs: the :meth:`QIPipelineWorkflow.run` inputs
    :param opts: the :class:`QIPipelineWorkflow` initializer options
    :return: the :meth:`QIPipelineWorkflow.run` result
    """
    return QIPipelineWorkflow(**opts).run(*inputs)


class QIPipelineWorkflow(object):
    """
    QIPipeline builds and executes the OHSU QIN workflows.
    The pipeline builds a composite workflow which stitches together
    the following constituent workflows:
    
    - staging: Prepare the new AIRC DICOM visits, as described in
        :class:`qipipe.staging.StagingWorkflow`
    
    - registration: Mask, register and reslice the staged images,
        as described in :class:`qipipe.staging.RegistrationWorkflow`
    
    - modeling: Perform PK modeling as described in
        :class:`qipipe.staging.ModelingWorkflow`
     """
    
    def __init__(self, **opts):
        """
        Builds the pipeline workflow.
        
        The default workflow settings can be overriden by a configuration
        file specified in the ```staging``, ``registration`` or ``modeling``
        option. If the ``registration`` option is set to False, then
        registration is skipped and modeling is performed on the staged
        scans. If the ``modeling`` option is set to False, then PK modeling
        is not performed.
        
        :param opts: the constituent workflow initialization options,
            augmented by the following options
        :keyword staging: the optional staging configuration file
        :keyword registration: the optional registration configuration file,
            or False to skip registration and model the staged scans
        :keyword modeling: the optional modeling configuration file, or
            False to skip modeling
        """
        self.reusable_workflow = self._create_reusable_workflow(**opts)
        """
        The reusable workflow.
        The reusable workflow can be embedded in an execution workflow.
        """
        
        self.execution_workflow = self._create_execution_workflow(**opts)
        """
        The execution workflow. The execution workflow is executed by calling
        the :meth:`qipipe.pipelines.modeling.QIPipelineWorkflow.run` method.
        """
    
    def run(self, collection, *inputs, **opts):
        """
        Runs the OHSU QIN pipeline on the the given AIRC subject directories.
        The supported AIRC collections are listed in
        :mod:`qipipe.staging.airc_collection`.
        
        This method returns a dictionary consisting of the following items:
        
        - ``sessions``: the (subject, session) name tuples of the new sessions
          created in XNAT

        - ``reconstruction``: the registration XNAT reconstruction name

        - ``analysis``: the PK mapping XNAT analysis name
        
        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories to stage
        :param opts: the :meth:`qipipe.pipelines.staging.StagingWorkflow.run` options
        :return: the pipeline result
        """
        # The staging location.
        if opts.has_key('dest'):
            dest = os.path.abspath(opts['dest'])
        else:
            dest = os.path.join(os.getcwd(), 'data')
        
        with xnat_helper.connection():
            # The overwrite option is only used for detecting new visits.
            overwrite = opts.pop('overwrite', False)
            # The new input (subject, session, series) tuples.
            series_specs = detect_new_visits(collection, *inputs, overwrite=overwrite)
            # The staging options.
            stg_opts = dict(base_dir=base_dir)
            # The dest staging input option.
            if 'dest' in opts:
                stg_opts['opts'] = opts.pop('dest')
            # Stage the input AIRC files.
            stg_result = staging.run(self.collection, *inputs, **stg_opts)
            # If there are no new visits to stage, then bail.
            if not stg_result:
                return []
            
            # If the registration flag is set to False, then return the staged XNAT sessions.
            if opts.get('registration') == False:
                logger.debug("Skipping registration since the registration option is set to False.")
                return session_specs
            # Register the images.
            reg_result = registration.run(*session_specs, base_dir=base_dir, **opts)
            
            # If the modeling flag is set to False, then return the registered XNAT reconstructions.
            if opts.get('modeling') == False:
                logger.debug("Skipping modeling since the modeling option is set to False.")
                return reg_result
            # Perform PK modeling.
            mdl_inputs = [dict(subject=sbj, session=sess, reconstruction=recon) for sbj, sess, recon in reg_specs]
            return modeling.run(*mdl_inputs, base_dir=base_dir, **opts)
    
    def _create_execution_workflow(self, base_dir=None, **opts):
        """
        Builds the executable pipeline workflow described in :class:`QIPipeline`.
        
        :param base_dir: the execution working directory (default is a new temp directory)
        :param opts: the additional reusable workflow options described in
            :meth:`__init__`
        :return: the Nipype workflow
        """
        logger.debug("Building the QIN pipeline execution workflow...")

        # The work directory used for all constituent workflows.
        if not opts.get('base_dir', None):
            base_dir = opts.pop('base_dir', None) or tempfile.mkdtemp()
        
        # The reusable workflow.
        reusable_wf = self._create_reusable_workflow(base_dir=base_dir, **opts)
        
        # The execution workflow.
        wf_name = reusable_wf.name + '_exec'
        exec_wf = pe.Workflow(name=wf_name, base_dir=base_dir)
        
        # The download fields.
        dl_fields = ['subject', 'session', 'reconstruction', 'container_type']
        # The reusable workflow input fields.
        reusable_fields = reusable_wf.get_node('input_spec').inputs.copyable_trait_names()
        in_fields = set(dl_fields).union(reusable_fields)
        # The input node.
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # The image download node.
        dl_images = pe.Node(XNATDownload(project=project()), name='dl_images')
        for field in dl_fields:
            exec_wf.connect(input_spec, field, dl_images, field)
        
        # Download the mask.
        dl_mask = pe.Node(XNATDownload(project=project(), reconstruction='mask'),
            name='dl_mask')
        exec_wf.connect(input_spec, 'subject', dl_mask, 'subject')
        exec_wf.connect(input_spec, 'session', dl_mask, 'session')
        
        # Model the images.
        exec_wf.connect(input_spec, 'subject', reusable_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', reusable_wf, 'input_spec.session')
        exec_wf.connect(dl_mask, 'out_file', reusable_wf, 'input_spec.mask_file')
        exec_wf.connect(dl_images, 'out_files', reusable_wf, 'input_spec.in_files')
        
        # Make the default XNAT assessment name. The name is unique, which permits
        # more than one model to be stored for each input series without a name
        # conflict.
        analysis = "%s_%s" % (PK_PREFIX, file_helper.generate_file_name())
        
        # The upload nodes.
        reusable_out_fields = reusable_wf.get_node('output_spec').outputs.copyable_trait_names()
        upload_node_dict = {field: self._create_output_upload_node(analysis, field)
            for field in reusable_out_fields}
        for field, node in upload_node_dict.iteritems():
            exec_wf.connect(input_spec, 'subject', node, 'subject')
            exec_wf.connect(input_spec, 'session', node, 'session')
            reusable_field = 'output_spec.' + field
            exec_wf.connect(reusable_wf, reusable_field, node, 'in_files')
        
        # Collect the execution workflow output fields.
        exec_out_fields = ['subject', 'session', 'analysis']
        output_spec = pe.Node(IdentityInterface(fields=exec_out_fields, analysis=analysis),
            name='output_spec')
        for field in ['subject', 'session']:
            exec_wf.connect(input_spec, field, output_spec, field)
        
        logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(reusable_wf)
        
        return exec_wf

    
    def _create_reusable_workflow(self, base_dir=None, **opts):
        """
        Builds the executable pipeline workflow described in :class:`QIPipeline`.
        
        :param base_dir: the execution working directory (default is a new temp directory)
        :param opts: the additional reusable workflow options described in
            :meth:`__init__`
        :return: the Nipype workflow
        """
        logger.debug("Building the QIN pipeline execution workflow...")
    