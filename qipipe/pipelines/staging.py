import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from ..interfaces import FixDicom, Compress, MapCTP, XNATUpload
from ..staging.staging_helper import iter_new_visits, group_dicom_files_by_series

import logging
logger = logging.getLogger(__name__)

def new_series_specs(collection, *subject_dirs):
    """
    Creates the series specifications for the new images in the given patient DICOM directories.
    Each specification is a (subject, session, series, dicom files) tuple.
    There is one specification for each new series.
    
    @param collection: the AIRC image collection name 
    @param subject_dirs: the directories to search for new visits
    @return: the new series specifications
    """
    
    # Collect the new AIRC visits.
    new_visits = list(iter_new_visits(collection, *subject_dirs))
    
    # The (subject, session, series, dicom files) inputs.
    ser_specs = []
    for sbj, sess, dcm_file_iter in new_visits:
        # Group the session DICOM input files by series.
        ser_dcm_dict = group_dicom_files_by_series(dcm_file_iter)
        # Collect the (subject, session, series, dicom_files) tuples.
        for ser, dcm in ser_dcm_dict.iteritems():
            logger.debug("The QIN workflow will iterate over subject %s session %s series %s." % (sbj, sess, ser))
            ser_specs.append((sbj, sess, ser, dcm))
    
    return ser_specs

def create_staging_connections(collection, series_spec):
    """
    Creates the staging connections.
    
    @param collection: the AIRC image collection name 
    @param series_spec: the series spec iteration node
    @return: the staging workflow connections
    """
    
    # The workflow input.
    input_spec = pe.Node(IdentityInterface(fields=['collection', 'dest', 'subjects']),
        name='input_spec')
            
    # Map each QIN Patient ID to a CTP Patient ID.
    map_ctp = pe.Node(MapCTP(), name='map_ctp')
    
    # The CTP staging directory factory.
    ctp_dir_func = Function(input_names=['dest', 'subject', 'session', 'series'],
        output_names=['out_dir'], function=_ctp_series_directory)
    ctp_dir = pe.Node(ctp_dir_func, name='ctp_dir')
    
    # Fix the AIRC DICOM tags.
    fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
    fix_dicom.inputs.collection = collection
    
    # Compress the fixed files.
    compress = pe.MapNode(Compress(), iterfield='in_file', name='compress')
    
    # Store the DICOM files in XNAT.
    store_dicom = pe.Node(XNATUpload(project='QIN', format='DICOM'), name='store_dicom')
    
    # Return the connections.
    return [
        (input_spec, map_ctp, [('collection', 'collection'), ('subjects', 'patient_ids'), ('dest', 'dest')]),
        (input_spec, fix_dicom, [('collection', 'collection')]),
        (input_spec, ctp_dir, [('dest', 'dest')]),
        (series_spec, ctp_dir, [('subject', 'subject'), ('session', 'session'), ('series', 'series')]),
        (series_spec, fix_dicom, [('subject', 'subject'), ('dicom_files', 'in_files')]),
        (series_spec, store_dicom, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
        (ctp_dir, compress, [('out_dir', 'dest')]),
        (fix_dicom, compress, [('out_files', 'in_file')]),
        (compress, store_dicom, [('out_file', 'in_files')])]

def _ctp_series_directory(dest, subject, session, series):
    """
    @return: the dest/subject/session/series directory path
    """
    import os
    return os.path.join(dest, subject, session, str(series))
