import os
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.dcmstack import DcmStack
from ..interfaces import Glue, FixDicom, Compress, MapCTP, XNATUpload
from ..staging.staging_error import StagingError
from ..staging.staging_helper import iter_new_visits, group_dicom_files_by_series
from ..helpers import xnat_helper

import logging
logger = logging.getLogger(__name__)

def run(collection, *subject_dirs, **opts):
    """
    Builds and runs the staging workflow.
    
    @param collection: the AIRC image collection name 
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: the workflow options
    @return: the new XNAT (subject, session) name tuples
    """

    # Collect the new AIRC visits into (subject, session, dicom_files)
    # tuples.
    new_visits = list(iter_new_visits(collection, *subject_dirs))
    # If there are no new images, then bail.
    if not new_visits:
        logger.info("No new images were detected.")
        return []
    logger.debug("%d new visits were detected" % len(new_visits))
    
    # Group the DICOM files by series.
    scan_specs = _group_sessions_by_series(*new_visits)

    # Make the staging workflow.
    wf = _create_workflow(collection, *scan_specs, **opts)
    
    # If debug is set, then diagram the staging workflow graph.
    if logger.level <= logging.DEBUG:
        if wf.base_dir:
            grf = os.path.join(wf.base_dir, 'staging.dot')
        else:
            grf = 'staging.dot'
        wf.write_graph(dotfilename=grf)
        logger.debug("The staging workflow graph is depicted at %s.png." % grf)
    
    # Run the staging workflow.
    with xnat_helper.connection():
        wf.run()
    
    # Return the new XNAT (subject, session) tuples.
    return [(sbj, sess) for sbj, sess, _ in new_visits]

def _create_workflow(collection, *scan_specs, **opts):
    """
    @param collection: the AIRC image collection name 
    @param scan_specs: the (subject, session, scan, dicom_files) tuples to stage
    @param opts: the workflow options
    @keyword dest: the destination directory (default current working directory)
    @return: the staging workflow, or None if there are no new images
    """
    msg = 'Creating the staging workflow'
    if opts:
        msg = msg + ' with options %s...' % opts
    logger.debug("%s...", msg)

    # The workflow.
    wf = pe.Workflow(name='staging', **opts)
    
    # The subjects with new sessions.
    subjects = {sbj for sbj, _, _, _ in scan_specs}

    # The iterable series specification.
    glue = Glue(input_names=['scan_spec'], output_names=['subject', 'session', 'scan', 'dicom_files'])
    scan_spec = pe.Node(glue, name='scan_spec')
    scan_spec.iterables = ('scan_spec', scan_specs)

    # The staging location.
    dest = os.path.abspath(opts.pop('dest', os.getcwd()))

    # The workflow input.
    input_spec = pe.Node(IdentityInterface(fields=['collection', 'dest', 'subjects']),
        name='input_spec')
    input_spec.inputs.collection = collection
    input_spec.inputs.dest = dest
    input_spec.inputs.subjects = subjects

    # Map each QIN Patient ID to a CTP Patient ID.
    map_ctp = pe.Node(MapCTP(), name='map_ctp')

    # The CTP staging directory factory.
    ctp_dir_func = Function(input_names=['dest', 'subject', 'session', 'series'], dest=dest,
        output_names=['out_dir'], function=_ctp_series_directory)
    ctp_dir = pe.Node(ctp_dir_func, name='ctp_dir')
    ctp_dir.inputs.dest = dest

    # Fix the AIRC DICOM tags.
    fix_dicom = pe.Node(FixDicom(collection=collection), name='fix_dicom')

    # Compress the fixed DICOM files.
    compress = pe.MapNode(Compress(), iterfield='in_file', name='compress')

    # Store the fixed DICOM files in XNAT.
    store_dicom = pe.Node(XNATUpload(project='QIN', format='DICOM'), name='store_dicom')

    # Stack the scan.
    stack = pe.Node(DcmStack(embed_meta=True, out_format="series%(SeriesNumber)03d"),
        name='stack')
    
    # Store the stack files in XNAT.
    store_stack = pe.Node(XNATUpload(project='QIN', format='NIFTI'), name='store_stack')
    
    wf.connect([
        (input_spec, map_ctp, [('collection', 'collection'), ('subjects', 'patient_ids'), ('dest', 'dest')]),
        (input_spec, ctp_dir, [('dest', 'dest')]),
        (scan_spec, ctp_dir, [('subject', 'subject'), ('session', 'session'), ('scan', 'series')]),
        (scan_spec, fix_dicom, [('subject', 'subject'), ('dicom_files', 'in_files')]),
        (scan_spec, store_dicom, [('subject', 'subject'), ('session', 'session'), ('scan', 'scan')]),
        (scan_spec, store_stack, [('subject', 'subject'), ('session', 'session'), ('scan', 'scan')]),
        (ctp_dir, compress, [('out_dir', 'dest')]),
        (fix_dicom, compress, [('out_files', 'in_file')]),
        (fix_dicom, stack, [('out_files', 'dicom_files')]),
        (compress, store_dicom, [('out_file', 'in_files')]),
        (stack, store_stack, [('out_file', 'in_files')])])
    
    return wf

def _group_sessions_by_series(*session_specs):
    """
    Creates the series specifications for the new images in the given sessions.

    @param session_specs: the (subject, session, dicom_files) tuples to group
    @return: the series (subject, session, series, dicom_files) tuples
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
    @return: the dest/subject/session/series directory path
    """
    import os
    return os.path.join(dest, subject, session, str(series))
