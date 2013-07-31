import os, tempfile
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from ..helpers import xnat_helper
from .pipeline_error import PipelineError
from .staging import detect_new_visits, StagingWorkflow
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
    return QIPipelineWorkflow(**opts).run(*inputs)


class QIPipelineWorkflow(object):
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
    ``workflow`` instance variable. The workflow input is a node
    named ``input_spec`` with the same fields as the staging ``input_spec``.
    
    In addition, there are two iterable inputs:
    
    - The ``iter_session`` node input field ``session_spec`` must be set
      to the (subject, session) input tuples
    
    - The ``iter_series`` node input field ``series_spec`` must be set
      to the (subject, session, scan, dicom_files) input tuples
    """
    
    def __init__(self, **opts):
        """
        Builds the pipeline workflow.
        
        The default workflow settings can be overriden by a configuration
        file specified in the ``staging``, ``mask``, ``registration`` or
        ``modeling`` option. If the ``mask`` option is set to False, then
        only staging is performed. If the ``registration`` option is set
        to false, then  registration is skipped and modeling is performed on
        the staged scans. If the ``modeling`` option is set to False, then
        PK modeling is not performed.
        
        :keyword staging: the optional staging configuration file
        :keyword mask: the optional mask configuration file, or False to
            halt after staging the files
        :keyword registration: the optional registration configuration file,
            or False to skip registration and model the staged scans
        :keyword modeling: the optional modeling configuration file, or
            False to skip modeling
        """
        self.workflow = self._create_workflow(**opts)
        """
        The pipeline execution workflow. The execution workflow is executed by
        calling the :meth:`qipipe.pipeline.modeling.QIPipelineWorkflow.run`
        method.
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
        :param opts: the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` options
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
            
            # If the mask flag is set to False, then return the staged XNAT sessions.
            if opts.get('mask') == False:
                logger.debug("Halting after staging since the mask option is set to False.")
                return session_specs
            
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
    
    def _create_workflow(self, **opts):
        """
        Builds the executable pipeline workflow described in :class:`QIPipeline`.
        
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
            mask_wf = reg_wf = mdl_wf = None
        else:
            mask_wf_gen = self._workflow_generator(MaskWorkflow, base_dir, mask_opt)
            mask_wf = mask_wf_gen.workflow()
            
            # The registration workflow.
            reg_opt = opts.get('registration', None)
            if reg_opt == False:
                reg_wf = None
            else:
                reg_wf_gen = self._workflow_generator(RegistrationWorkflow, base_dir, reg_opt)
                reg_wf = reg_wf_gen.reusable_workflow()
            
            # The modeling workflow.
            mdl_opt = opts.get('modeling', None)
            if mdl_opt == False:
                mdl_wf = None
            else:
                mdl_wf_gen = self._workflow_generator(ModelingWorkflow, base_dir, mdl_opt)
                mdl_wf = mdl_wf_gen.reusable_workflow
        
        # The non-iterable workflow inputs.
        in_fields = ['collection', 'dest', 'subjects']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        logger.debug("The QIN pipeline non-iterable input is %s with fields %s" %
            (input_spec.name, input_fields))
        exec_wf.connect(input_spec, 'collection', stg_wf, 'input_spec.collection')
        exec_wf.connect(input_spec, 'dest', stg_wf, 'input_spec.dest')
        exec_wf.connect(input_spec, 'subjects', stg_wf, 'input_spec.subjects')
        
        # The iterable session input.
        iter_session_xf = Unpack(input_name='session_spec',
            output_names=['subject', 'session'])
        iter_session = pe.Node(iter_session_xf, name='iter_session')
        logger.debug("The QIN pipeline iterable series input is %s with fields %s" %
            (iter_session.name, iter_session.inputs.copyable_trait_names()))
        exec_wf.connect(iter_session, 'session_spec', stg_wf, 'iter_session.session_spec')
        
        # The iterable series input.
        iter_series_xf = Unpack(input_name='series_spec',
            output_names=['scan', 'dicom_files'])
        iter_series = pe.Node(iter_series_xf, name='iter_series')
        iter_series.itersource = 'iter_session'
        logger.debug("The QIN pipeline iterable series input is %s with fields %s" %
            (iter_series.name, iter_series.inputs.copyable_trait_names()))
        exec_wf.connect(iter_series, 'series_spec', stg_wf, 'iter_series.series_spec')
        
        # Stitch together the workflows.
        if mask_wf:
            staged = pe.Node(IdentityInterface(fields=['subject', 'session', 'image']), name='staged')
            exec_wf.connect(iter_series, 'subject', mask_wf, 'input_spec.subject')
            exec_wf.connect(iter_series, 'session', mask_wf, 'input_spec.session')
            exec_wf.connect(stg_wf, 'output_spec.out_file', staged, 'image')
            
            
            # The iterable session node.
            iter_session_xf = Unpack(input_name='session_spec',
                output_names=['subject', 'session'])
            exec_wf.connect(iter_series, 'subject', mask_wf, 'input_spec.subject')
            exec_wf.connect(iter_series, 'session', mask_wf, 'input_spec.session')
            images = pe.JoinNode(IdentityInterface(fields=['subject', 'session', 'images']), name='images',
                joinsource='iter_series')
            exec_wf.connect(stg_wf, 'output_spec.out_file', images, 'images')
            exec_wf.connect(images, 'images', mask_wf, 'input_spec.images')
            if reg_wf:
                exec_wf.connect(iter_series, ('session_spec', _select_from, [0, 1]),
                    iter_session, 'session_spec')
                exec_wf.connect(stg_wf, 'input_spec.session', iter_session, 'session')
                exec_wf.connect(iter_session, 'session_spec', stg_wf, 'iter_series.series_spec')
                exec_wf.connect(input_spec, 'subject', reg_wf, 'input_spec.subject')
                exec_wf.connect(input_spec, 'session', reg_wf, 'input_spec.sess')
                exec_wf.connect(images, 'images', reg_wf, 'input_spec.images')
                exec_wf.connect(mask_wf, 'output_spec.out_file', reg_wf, 'input_spec.mask')
                exec_wf.connect(stg_wf, 'output_spec.out_file', reg_wf, 'image_iter.image')
            if mdl_wf:
                iter_session = pe.Node(iter_session_xf, name='iter_session')
                logger.debug("The QIN pipeline iterable session input is %s with fields %s" %
                    (iter_session.name, iter_session.inputs.copyable_trait_names()))
                for field in mdl_input_fields:
                    exec_wf.connect(input_spec, field, mdl_wf, '')
                if reg_wf:
                    exec_wf.connect(reg_wf, 'output_spec.realigned', mdl_wf, 'image_iter.image')
                else:
                    # Model the input scans rather than the registered images.
                    mdl_input.inputs.container_type = 'scan'
        
        
        
        
        # The workflow inputs.
        self.input_nodes = [sess_spec]
        input_spec = pe.Node(IdentityInterface(fields=['collection', 'dest', 'subjects']),
            name='input_spec')
        iter_series = pe.Node(IdentityInterface(fields=['subject', 'session', 'series', 'images']),
            name='iter_series')
        iter_image = pe.Node(IdentityInterface(fields=['image']),
            name='iter_image')
        
        
        
        
                
        
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


def _collect_session_images(dictionary, subject, session, image):
    dictionary[(subject, session)].append(image)

def _select_from(bunch, indexes):
    """
    return a tuple consisting of the items in the given bunch
    at the given indexes
    
    Examples
    --------
    >>> _select_from('abcd', [1, 3])
    ('b', 'd')
    """
    return tuple([bunch[i] for i in indexes])