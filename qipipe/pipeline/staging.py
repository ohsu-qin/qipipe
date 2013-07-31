import os
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.dcmstack import DcmStack
from ..helpers.project import project
from ..interfaces import Unpack, FixDicom, Compress, MapCTP, XNATUpload
from ..staging.staging_error import StagingError
from ..staging.staging_helper import subject_for_directory, iter_new_visits, group_dicom_files_by_series
from ..helpers import xnat_helper
from .pipeline_error import PipelineError
from .workflow_base import WorkflowBase
    
import logging
logger = logging.getLogger(__name__)

    
def detect_new_visits(collection, *inputs, **opts):
    """
    Detects which AIRC visits in the given input directories have not yet
    been stored into XNAT. The new visit images are grouped by series.
    
    :Note: If the `overwrite` option is set, then existing XNAT subjects
        which correspond to subjects in the input directories are deleted.
    
    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories
    :keyword overwrite: flag indicating whether to replace existing XNAT subjects
    :return: the new series (subject, session, series, dicom_files) tuples
    """
    # If the overwrite option is set, then delete existing subjects.
    if opts.get('overwrite'):
        subjects = [subject_for_directory(collection, d) for d in inputs]
        xnat_helper.delete_subjects(*subjects)
    
    # Collect the new AIRC visits into (subject, session, dicom_files)
    # tuples.
    new_visits = list(iter_new_visits(collection, *inputs))
    # If there are no new images, then bail.
    if not new_visits:
        logger.info("No new images were detected.")
        return []
    logger.debug("%d new visits were detected" % len(new_visits))
    
    # Group the DICOM files by series.
    return _group_sessions_by_series(*new_visits)

def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.staging.StagingWorkflow` and runs it
    on the given inputs.
    
    :param inputs: the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` inputs
    :param opts: the :class:`qipipe.pipeline.staging.StagingWorkflow` initializer options
    :return: the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` result
    """
    return StagingWorkflow(**opts).run(*inputs)


class StagingWorkflow(WorkflowBase):
    """
    The StagingWorkflow builds and executes the staging Nipype workflow.
    The staging workflow includes the following steps:
    
    - Group the input DICOM images into series.
    
    - Fix each input DICOM file header using the
      :class:`qipipe.interfaces.fix_dicom.FixDicom` interface.
    
    - Compress each fixed DICOM file.
    
    - Upload the fixed DICOM files into XNAT.
    
    - Stack each new series's 2-D DICOM files into a 3-D series NiFTI file
      using the DcmStack_ interface.
    
    - Upload each new series stack into XNAT.
    
    - Make the CTP_ QIN-to-TCIA subject id map.
    
    - Collect the id map and the compressed DICOM images into a target directory in
      collection/subject/session/series format for TCIA upload.
    
    The staging workflow input is the ``input_spec`` node consisting of the
    following input fields:
    
    - `collection`: the AIRC collection name
    
    - `dest`: the staging destination directory
    
    - `subjects`: the subjects to stage
    
    In addition, the iterable `iter_series` node input `series_spec` field
    must be set to the (subject, session, scan, dicom_files) staging input tuples.
    
    The staging workflow output is the `output_spec` node consisting of the
    following output fields:
    
    - `out_file`: the series stack NiFTI image file
    
    .. _CTP: https://wiki.cancerimagingarchive.net/display/Public/Image+Submitter+Site+User%27s+Guide
    .. _DcmStack: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.dcmstack.html
    """
    
    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow settings in
        that file override the default settings.
        
        :keyword base_dir: the workflow execution directory (default a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        """
        super(StagingWorkflow, self).__init__(logger, opts.pop('cfg_file', None))
        
        self.reusable_workflow = self._create_reusable_workflow(**opts)
        """
        The reusable staging workflow.
        The reusable workflow can be embedded in an execution workflow.
        """
        
        self.execution_workflow = self._create_execution_workflow(**opts)
        """
        The execution workflow. The execution workflow is executed by calling
        the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` method.
        """
    
    def run(self, collection, *inputs, **opts):
        """
        Builds and runs the staging workflow on the new AIRC visits in the given
        inputs.
        
        This method can be used with an alternate workflow specified by the
        `workflow` option. The workflow is required to implement the same
        input nodes and fields as this  execution workflow.
        
        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories to stage
        :param opts: the following workflow execution options
        :keyword dest: the TCIA upload destination directory (default current working
            directory)
        :keyword overwrite: the :meth:`new_series` overwrite flag
        :keyword workflow: the workflow to run
            (default is the execution workflow)
        :return: the new XNAT (subject, session) name tuples
        """
        # Group the new DICOM files by series.
        overwrite = opts.get('overwrite', False)
        new_series = detect_new_visits(collection, *inputs, overwrite=overwrite)
        
        # The subjects with new sessions.
        subjects = {sbj for sbj, _, _, _ in new_series}
        
        # The staging location.
        if opts.has_key('dest'):
            dest = os.path.abspath(opts['dest'])
        else:
            dest = os.path.join(os.getcwd(), 'data')
        
        # Set the workflow (collection, destination, subjects) input.
        exec_wf = self.execution_workflow
        input_spec = exec_wf.get_node('input_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.dest = dest
        input_spec.inputs.subjects = subjects
        
        # Set the iterable series inputs.
        iter_series = exec_wf.get_node('iter_series')
        iter_series.iterables = ('series_spec', new_series)
        
        # Run the staging workflow.
        self._run_workflow(exec_wf)
        
        # Return the new XNAT (subject, session) tuples.
        return {(sbj, sess) for sbj, sess, _, _ in new_series}
    
    def _create_execution_workflow(self, **opts):
        """
        :param opts: the workflow options described below
        :keyword base_dir: the workflow execution directory
            (default current directory)
        :keyword dest: the staging target directory
            (default base_dir ``data`` subdirectory)
        :return: the staging workflow
        """
        logger.debug("Creating the staging execution workflow...")
        
        # The reusable registration workflow.
        base_wf = self.reusable_workflow.clone(name='staging_base')
        
        # The execution workflow.
        exec_wf = pe.Workflow(name='exec_staging', base_dir=opts.get('base_dir'))
        
        # The non-iterable input.
        in_fields = ['collection', 'dest', 'subjects']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        exec_wf.connect(input_spec, 'collection', base_wf, 'input_spec.collection')
        exec_wf.connect(input_spec, 'dest', base_wf, 'input_spec.dest')
        exec_wf.connect(input_spec, 'subjects', base_wf, 'input_spec.subjects')
        
        # The iterable series node.
        iter_series_xf = Unpack(input_name='series_spec',
            output_names=['subject', 'session', 'scan', 'dicom_files'])
        iter_series = pe.Node(iter_series_xf, name='iter_series')
        exec_wf.connect(iter_series, 'subject', base_wf, 'iter_series.subject')
        exec_wf.connect(iter_series, 'session', base_wf, 'iter_series.session')
        exec_wf.connect(iter_series, 'scan', base_wf, 'iter_series.scan')
        exec_wf.connect(iter_series, 'dicom_files', base_wf, 'iter_series.dicom_files')
        
        return exec_wf
    
    def _create_reusable_workflow(self, **opts):
        """
        :param opts: the workflow options described below
        :keyword base_dir: the workflow execution directory
            (default current directory)
        :keyword dest: the staging target directory
            (default base_dir ``data`` subdirectory)
        :return: the staging workflow
        """
        logger.debug("Creating the staging workflow to embed in an execution "
            "workflow...")
        
        # The work directory.
        if opts.has_key('base_dir'):
            base_dir = os.path.abspath(opts['base_dir'])
        else:
            base_dir = os.getcwd()
        
        # The workflow.
        workflow = pe.Workflow(name='staging', base_dir=base_dir)
        
        # The workflow (collection, directory, subjects) input.
        in_fields = ['collection', 'dest', 'subjects']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
            name='input_spec')
        logger.debug("The staging workflow non-iterable input is %s"
            " with fields %s" % (input_spec.name, in_fields))
        
        # The iterable series node.
        series_fields = ['subject', 'session', 'scan', 'dicom_files']
        iter_series = pe.Node(IdentityInterface(fields=series_fields),
            name='iter_series')
        logger.debug("The staging workflow iterable input is %s with fields %s"
            % (iter_series.name, iter_series.inputs.copyable_trait_names()))
        
        # Map each QIN Patient ID to a TCIA Patient ID for upload using CTP.
        map_ctp = pe.Node(MapCTP(), name='map_ctp')
        workflow.connect(input_spec, 'collection', map_ctp, 'collection')
        workflow.connect(input_spec, 'subjects', map_ctp, 'patient_ids')
        workflow.connect(input_spec, 'dest', map_ctp, 'dest')
        
        # The CTP staging directory factory.
        staging_dir_func = Function(
            input_names=['dest', 'subject', 'session', 'series'],
            output_names=['out_dir'], function=_make_series_staging_directory)
        staging_dir = pe.Node(staging_dir_func, name='staging_dir')
        workflow.connect(input_spec, 'dest', staging_dir, 'dest')
        workflow.connect(iter_series, 'subject', staging_dir, 'subject')
        workflow.connect(iter_series, 'session', staging_dir, 'session')
        workflow.connect(iter_series, 'scan', staging_dir, 'series')
        
        # Fix the AIRC DICOM tags.
        fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
        workflow.connect(input_spec, 'collection', fix_dicom, 'collection')
        workflow.connect(iter_series, 'subject', fix_dicom, 'subject')
        workflow.connect(iter_series, 'dicom_files', fix_dicom, 'in_files')
        
        # Compress each fixed DICOM file for a given series.
        # The result is a list of the compressed files for the series.
        compress_dicom = pe.MapNode(Compress(), iterfield='in_file',
            name='compress_dicom')
        workflow.connect(fix_dicom, 'out_files', compress_dicom, 'in_file')
        workflow.connect(staging_dir, 'out_dir', compress_dicom, 'dest')
        
        # Store the compressed scan DICOM files in XNAT.
        upload_dicom = pe.Node(XNATUpload(project=project(), format='DICOM'),
            name='upload_dicom')
        workflow.connect(iter_series, 'subject', upload_dicom, 'subject')
        workflow.connect(iter_series, 'session', upload_dicom, 'session')
        workflow.connect(iter_series, 'scan', upload_dicom, 'scan')
        workflow.connect(compress_dicom, 'out_file', upload_dicom, 'in_files')
        
        # Stack the scan.
        stack = pe.Node(DcmStack(embed_meta=True,
            out_format="series%(SeriesNumber)03d"), name='stack')
        workflow.connect(fix_dicom, 'out_files', stack, 'dicom_files')
        
        # Store the stack files in XNAT.
        upload_stack = pe.Node(XNATUpload(project=project(), format='NIFTI'),
            name='upload_stack')
        workflow.connect(iter_series, 'subject', upload_stack, 'subject')
        workflow.connect(iter_series, 'session', upload_stack, 'session')
        workflow.connect(iter_series, 'scan', upload_stack, 'scan')
        
        workflow.connect(stack, 'out_file', upload_stack, 'in_files')
        
        # The output is the series stack file.
        output_spec = pe.Node(IdentityInterface(fields=['out_file']),
            name='output_spec')
        workflow.connect(stack, 'out_file', output_spec, 'out_file')
        
        return workflow


def _group_sessions_by_series(self, *session_specs):
    """
    Creates the series specifications for the new images in the given sessions.
    
    :param session_specs: the (subject, session, dicom_files) tuples to group
    :return: the series (subject, session, series, dicom_files) tuples
    """
    
    # The (subject, session, series, dicom files) inputs.
    ser_specs = []
    for sbj, sess, dcm_file_iter in session_specs:
        # Group the session DICOM input files by series.
        ser_dcm_dict = group_dicom_files_by_series(dcm_file_iter)
        if not ser_dcm_dict:
            raise StagingError("No DICOM files were detected in the"
                " %s %s session source directory." % (sbj, sess))
        # Collect the (subject, session, series, dicom_files) tuples.
        for ser, dcm in ser_dcm_dict.iteritems():
            logger.debug("The staging workflow will iterate over subject %s"
                " session %s series %s." % (sbj, sess, ser))
            ser_specs.append((sbj, sess, ser, dcm))
    
    return ser_specs

def _make_series_staging_directory(dest, subject, session, series):
    """
    Returns the dest/subject/session/series directory path in which to place
    DICOM files for TCIA upload. Creates the directory, if necessary.
    
    :return: the target series directory path
    """
    import os
    path = os.path.join(dest, subject, session, str(series))
    if not os.path.exists(path):
        os.makedirs(path)
    
    return path
