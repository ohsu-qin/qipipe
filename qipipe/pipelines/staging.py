import os
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.dcmstack import DcmStack
from ..interfaces import Glue, FixDicom, Compress, MapCTP, XNATUpload
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
    @return: the new XNAT scans
    """

    # Collect the new AIRC visits into (subject, session, dicom_files)
    # tuples.
    new_visits = list(iter_new_visits(collection, *subject_dirs))
    # If there are no new images, then bail.
    if not new_visits:
        logger.info("No new images were detected.")
        return

    # Make the staging workflow.
    wf = _create_workflow(collection, *new_visits, **opts)
    
    # If debug is set, then diagram the staging workflow graph.
    if logger.level <= logging.DEBUG:
        grf = os.path.join(wf.base_dir, 'staging.dot')
        wf.write_graph(dotfilename=grf)
        logger.debug("The staging workflow graph is depicted at %s.png." % grf)
    
    # Run the staging workflow.
    wf.run()
    
    # Return the new XNAT sessions.
    xnat = xnat_helper.facade()
    return [xnat.get_session('QIN', subject=sbj, session=sess) for sbj, sess, _ in new_visits]

def _create_workflow(collection, *session_specs, **opts):
    """
    @param collection: the AIRC image collection name 
    @param session_specs: the (subject, session, dicom_files) tuples to stage
    @param opts: the workflow options
    @keyword dest: the destination directory (default current working directory)
    @return: the staging workflow, or None if there are no new images
    """
    
    msg = 'Creating the staging workflow'
    if opts:
        msg = msg + ' with options %s...' % opts
    logger.debug("%s...", msg)

    # The staging location.
    dest = os.path.abspath(opts.pop('dest', os.getcwd()))

    # The workflow.
    wf = pe.Workflow(name='staging', **opts)
    
    
    # The subjects with new sessions.
    subjects = {sbj for sbj, _, _ in session_specs}
    
    # Group the series.
    series_specs = _group_sessions_by_series(*session_specs)

    # The iterable series specification.
    glue = Glue(input_names=['series_spec'], output_names=['subject', 'session', 'series', 'dicom_files'])
    series_spec = pe.Node(glue, name='series_spec')
    series_spec.iterables = ('series_spec', series_specs)

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
    compress_fixed = pe.MapNode(Compress(), iterfield='in_file', name='compress_fixed')

    # Store the fixed DICOM files in XNAT.
    store_dicom = pe.Node(XNATUpload(project='QIN', format='DICOM'), name='store_dicom')

    # Stack the series.
    stack = pe.Node(DcmStack(embed_meta=True, out_format="series%(SeriesNumber)03d"),
        name='stack')

    # Compress the stack files.
    compress_stack = pe.Node(Compress(), name='compress_stack')
    
    # Store the stack files in XNAT.
    store_stack = pe.Node(XNATUpload(project='QIN', format='NIFTI'), name='store_stack')
    
    wf.connect([
        (input_spec, map_ctp, [('collection', 'collection'), ('subjects', 'patient_ids'), ('dest', 'dest')]),
        (input_spec, ctp_dir, [('dest', 'dest')]),
        (series_spec, ctp_dir, [('subject', 'subject'), ('session', 'session'), ('series', 'series')]),
        (series_spec, fix_dicom, [('subject', 'subject'), ('dicom_files', 'in_files')]),
        (series_spec, store_dicom, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
        (series_spec, store_stack, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
        (ctp_dir, compress_fixed, [('out_dir', 'dest')]),
        (fix_dicom, compress_fixed, [('out_files', 'in_file')]),
        (fix_dicom, stack, [('out_files', 'dicom_files')]),
        (compress_fixed, store_dicom, [('out_file', 'in_files')]),
        (stack, compress_stack, [('out_file', 'in_file')]),
        (compress_stack, store_stack, [('out_file', 'in_files')])])
    
    return wf

def _group_sessions_by_series(*session_specs):
    """
    Creates the series specifications for the new images in the given sessions.

    @param session_specs: the (subject, session, dicom_files) tuples to group
    @return: the new series specifications
    """

    # The (subject, session, series, dicom files) inputs.
    ser_specs = []
    for sbj, sess, dcm_file_iter in session_specs:
        # Group the session DICOM input files by series.
        ser_dcm_dict = group_dicom_files_by_series(dcm_file_iter)
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
