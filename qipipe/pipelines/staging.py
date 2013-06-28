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

import logging
logger = logging.getLogger(__name__)

def run(collection, *subject_dirs, **opts):
    """
    Builds and runs the staging workflow.
    
    :param collection: the AIRC image collection name
    :param subject_dirs: the AIRC source subject directories to stage
    :param opts: the :meth:`create_workflow` options
    :return: the new XNAT (subject, session) name tuples
    """
    
    # If the force option is set, then delete existing subjects.
    if opts.pop('force', False):
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
    series_specs = _group_sessions_by_series(*new_visits)
    
    # Make the staging workflow.
    workflow = create_workflow(collection, *series_specs, **opts)
    
    # If debug is set, then diagram the staging workflow graph.
    if logger.level <= logging.DEBUG:
        if workflow.base_dir:
            grf = os.path.join(workflow.base_dir, 'staging.dot')
        else:
            grf = 'staging.dot'
        workflow.write_graph(dotfilename=grf)
        logger.debug("The staging workflow graph is depicted at %s.png." % grf)
    
    # Run the staging workflow.
    with xnat_helper.connection():
        workflow.run()
    
    # Return the new XNAT (subject, session) tuples.
    return [(sbj, sess) for sbj, sess, _ in new_visits]

def create_workflow(collection, *series_specs, **opts):
    """
    :param collection: the AIRC image collection name
    :param series_specs: the (subject, session, scan, dicom_files) tuples to stage
    :param opts: the workflow options described below
    :keyword base_dir: the workflow execution directory (default current directory)
    :keyword config: the workflow inputs configuration file 
    :keyword dest: the destination directory
        (default is workflow execution ``data`` subdirectory)
    :return: the staging workflow, or None if there are no new images
    """
    msg = 'Creating the staging workflow'
    if opts:
        msg = msg + ' with options %s...' % opts
    logger.debug("%s...", msg)
    
    base_dir = opts.get('base_dir', None) or os.getcwd()
    
    # The staging location.
    dest = os.path.abspath(opts.pop('dest', None) or os.path.join(base_dir, 'data'))
    
    # The workflow.
    workflow = pe.Workflow(name='staging', base_dir=base_dir)
    
    # The subjects with new sessions.
    subjects = {sbj for sbj, _, _, _ in series_specs}
    
    # The workflow (collection, directory, subjects) input.
    subject_spec = pe.Node(IdentityInterface(fields=['collection', 'dest', 'subjects']),
        name='subject_spec')
    subject_spec.inputs.collection = collection
    subject_spec.inputs.dest = dest
    subject_spec.inputs.subjects = subjects
    
    # The iterable series inputs.
    series_spec_xf = Unpack(input_name='series_spec', output_names=['subject', 'session', 'scan', 'dicom_files'])
    series_spec = pe.Node(series_spec_xf, name='series_spec')
    series_spec.iterables = ('series_spec', series_specs)
    
    # Map each QIN Patient ID to a CTP Patient ID.
    map_ctp = pe.Node(MapCTP(), name='map_ctp')
    workflow.connect(subject_spec, 'collection', map_ctp, 'collection')
    workflow.connect(subject_spec, 'subjects', map_ctp, 'patient_ids')
    workflow.connect(subject_spec, 'dest', map_ctp, 'dest')
    
    # The CTP staging directory factory.
    ctp_dir_func = Function(input_names=['dest', 'subject', 'session', 'series'], dest=dest,
        output_names=['out_dir'], function=_ctp_series_directory)
    ctp_dir = pe.Node(ctp_dir_func, name='ctp_dir')
    workflow.connect(subject_spec, 'dest', ctp_dir, 'dest')
    workflow.connect(series_spec, 'subject', ctp_dir, 'subject')
    workflow.connect(series_spec, 'session', ctp_dir, 'session')
    workflow.connect(series_spec, 'scan', ctp_dir, 'series')
    
    # Fix the AIRC DICOM tags.
    fix_dicom = pe.Node(FixDicom(collection=collection), name='fix_dicom')
    workflow.connect(series_spec, 'subject', fix_dicom, 'subject')
    workflow.connect(series_spec, 'dicom_files', fix_dicom, 'in_files')
    
    # Compress each fixed DICOM file for a given series.
    # The result is a list of the compressed files for the series.
    compress_dicom = pe.MapNode(Compress(), iterfield='in_file', name='compress_dicom')
    workflow.connect(fix_dicom, 'out_files', compress_dicom, 'in_file')
    workflow.connect(ctp_dir, 'out_dir', compress_dicom, 'dest')
    
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

def _group_sessions_by_series(*session_specs):
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

def _ctp_series_directory(dest, subject, session, series):
    """
    :return: the dest/subject/session/series directory path
    """
    import os
    return os.path.join(dest, subject, session, str(series))
