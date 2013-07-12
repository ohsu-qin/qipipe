import os
import nipype.pipeline.engine as pe
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


def run(*inputs, **opts):
    """
    Creates a :class:`StagingWorkflow` and runs its on the given inputs.
    
    :param inputs: the :meth:`StagingWorkflow.run` inputs
    :param opts: the :class:`StagingWorkflow` initializer options
    :return: the :meth:`StagingWorkflow.run` result
    """
    return StagingWorkflow(**opts).run(*inputs)


class StagingWorkflow(WorkflowBase):
    """StagingWorkflow builds and executes the staging Nipype workflow."""
    
    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow settings in
        that file override the default settings.
        
        :param opts: the following options
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword base_dir: the workflow execution directory (default current directory)
        """
        super(StagingWorkflow, self).__init__(logger, opts.pop('cfg_dir', None))
        
        self.workflow = self._create_workflow(**opts)
        """
        The execution workflow.
        The workflow is executed by calling the :meth:`run` method.
        
        :Note: the execution workflow cannot be directly connected from another workflow,
            since execution iterates over each image. A Nipype iterator is defined when
            the workflow is built, and cannot be set dynamically during execution.
            Consequently, the session images input must be wired into the workflow
            and cannot be set dynamically from the output of a parent workflow node.
        """
    
    def run(self, collection, *subject_dirs, **opts):
        """
        Builds and runs the staging workflow.
        
        :param collection: the AIRC image collection name
        :param subject_dirs: the AIRC source subject directories to stage
        :param opts: the :meth:`create_workflow` options as well as the following:
        :keyword overwrite: flag indicating whether to replace existing XNAT subjects
        :return: the new XNAT (subject, session) name tuples
        """
        # If the force option is set, then delete existing subjects.
        if opts.pop('overwrite', False):
            subjects = [subject_for_directory(collection, d) for d in subject_dirs]
            xnat_helper.delete_subjects(*subjects)
        
        # Collect the new AIRC visits into (subject, session, dicom_files)
        # tuples.
        new_visits = list(iter_new_visits(collection, *subject_dirs))
        # If there are no new images, then bail.
        if not new_visits:
            logger.info("No new images were detected.")
            return []
        logger.debug("%d new visits were detected" % len(new_visits))
        
        # Group the DICOM files by series.
        series_specs = self._group_sessions_by_series(*new_visits)
        
        # The subjects with new sessions.
        subjects = {sbj for sbj, _, _, _ in series_specs}
        
        # Set the workflow (collection, destination, subjects) input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.dest = dest
        input_spec.inputs.subjects = subjects
        
        # Set the iterable series inputs.
        series_spec = self.workflow.get_node('series_spec')
        series_spec.iterables = ('series_spec', series_specs)
        
        # Run the staging workflow.
        self._run_workflow(workflow)
        
        # Return the new XNAT (subject, session) tuples.
        return [(sbj, sess) for sbj, sess, _ in new_visits]
    
    def _create_workflow(self, **opts):
        """
        :param opts: the workflow options described below
        :keyword base_dir: the workflow execution directory (default current directory)
        :keyword dest: the staging target directory (default base_dir ``data`` subdirectory)
        :return: the staging workflow
        """
        logger.debug("Creating the staging workflow...")
        
        # The work directory.
        if opts.has_key('base_dir'):
            base_dir = os.path.abspath(opts['base_dir'])
        else:
            base_dir = os.getcwd()
        
        # The staging location.
        if opts.has_key('dest'):
            dest = os.path.abspath(opts['dest'])
        else:
            dest = os.path.join(base_dir, 'data')
        
        # The workflow.
        workflow = pe.Workflow(name='staging', base_dir=base_dir)
        
        # The workflow (collection, directory, subjects) input.
        input_spec = pe.Node(IdentityInterface(fields=['collection', 'dest', 'subjects']),
            name='subject_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.dest = dest
        input_spec.inputs.subjects = subjects
        
        # The iterable series inputs.
        series_spec_xf = Unpack(input_name='series_spec', output_names=['subject', 'session', 'scan', 'dicom_files'])
        series_spec = pe.Node(series_spec_xf, name='series_spec')
        
        # Map each QIN Patient ID to a CTP Patient ID.
        map_ctp = pe.Node(MapCTP(), name='map_ctp')
        workflow.connect(subject_spec, 'collection', map_ctp, 'collection')
        workflow.connect(subject_spec, 'subjects', map_ctp, 'patient_ids')
        workflow.connect(subject_spec, 'dest', map_ctp, 'dest')
        
        # The CTP staging directory factory.
        staging_dir_func = Function(input_names=['dest', 'subject', 'session', 'series'], dest=dest,
            output_names=['out_dir'], function=_make_series_staging_directory)
        staging_dir = pe.Node(staging_dir_func, name='staging_dir')
        workflow.connect(subject_spec, 'dest', staging_dir, 'dest')
        workflow.connect(series_spec, 'subject', staging_dir, 'subject')
        workflow.connect(series_spec, 'session', staging_dir, 'session')
        workflow.connect(series_spec, 'scan', staging_dir, 'series')
        
        # Fix the AIRC DICOM tags.
        fix_dicom = pe.Node(FixDicom(collection=collection), name='fix_dicom')
        workflow.connect(series_spec, 'subject', fix_dicom, 'subject')
        workflow.connect(series_spec, 'dicom_files', fix_dicom, 'in_files')
        
        # Compress each fixed DICOM file for a given series.
        # The result is a list of the compressed files for the series.
        compress_dicom = pe.MapNode(Compress(), iterfield='in_file', name='compress_dicom')
        workflow.connect(fix_dicom, 'out_files', compress_dicom, 'in_file')
        workflow.connect(staging_dir, 'out_dir', compress_dicom, 'dest')
        
        # Store the compressed scan DICOM files in XNAT.
        upload_dicom = pe.Node(XNATUpload(project=project(), format='DICOM'), name='upload_dicom')
        workflow.connect(series_spec, 'subject', upload_dicom, 'subject')
        workflow.connect(series_spec, 'session', upload_dicom, 'session')
        workflow.connect(series_spec, 'scan', upload_dicom, 'scan')
        workflow.connect(compress_dicom, 'out_file', upload_dicom, 'in_files')
        
        # Stack the scan.
        stack = pe.Node(DcmStack(embed_meta=True, out_format="series%(SeriesNumber)03d"),
            name='stack')
        workflow.connect(fix_dicom, 'out_files', stack, 'dicom_files')
        
        # Store the stack files in XNAT.
        upload_stack = pe.Node(XNATUpload(project=project(), format='NIFTI'), name='upload_stack')
        workflow.connect(series_spec, 'subject', upload_stack, 'subject')
        workflow.connect(series_spec, 'session', upload_stack, 'session')
        workflow.connect(series_spec, 'scan', upload_stack, 'scan')
        workflow.connect(stack, 'out_file', upload_stack, 'in_files')
        
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
                raise StagingError("No DICOM files were detected in the %s %s session source directory." % (sbj, sess))
            # Collect the (subject, session, series, dicom_files) tuples.
            for ser, dcm in ser_dcm_dict.iteritems():
                logger.debug("The staging workflow will iterate over subject %s session %s series %s." % (sbj, sess, ser))
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
