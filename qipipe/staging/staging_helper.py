"""Pipeline utility functions."""

import os, re, glob
from ..helpers import xnat_helper
from ..helpers.dicom_helper import iter_dicom_headers
from .staging_error import StagingError
from . import airc_collection as airc

import logging
logger = logging.getLogger(__name__)

SUBJECT_FMT = '%s%03d'
"""The QIN subject name format with arguments (collection, subject number)."""

SESSION_FMT = '%s_Session%02d'
"""The QIN series name format with arguments (subject, series number)."""

def iter_new_visits(collection, *subject_dirs):
    """
    Iterates over the visits in the given subject directories which are not in XNAT.
    Each iteration item is a (subject, session, dicom_file_iterator) tuple, formed as follows:
        - The subject is the XNAT subject ID formatted by L{SUBJECT_FMT}
        - The session is the XNAT experiment name formatted by L{SESSION_FMT}
        _ The DICOM files iterator iterates over the files which match the
          L{qipipe.staging.airc_collection} DICOM file include pattern
    
    The supported AIRC collections are defined L{qipipe.staging.airc_collection}.
    
    @param collection: the AIRC image collection name
    @param subject_dirs: the subject directories over which to iterate
    """
    
    # The AIRC collection with the given name.
    airc_coll = airc.collection_with_name(collection)
    # The visit subdirectory match pattern.
    vpat = airc_coll.session_pattern

    with xnat_helper.connection() as xnat:
        for d in subject_dirs:
            d = os.path.abspath(d)
            # Make the XNAT subject name.
            sbj_nbr = airc_coll.path2subject_number(d)
            sbj = SUBJECT_FMT % (collection, sbj_nbr)
            # The subject subdirectories which match the visit pattern.
            vmatches = [v for v in os.listdir(d) if re.match(vpat, v)]
            # Generate the new (subject, session, DICOM files) items in each visit.
            for v in vmatches:
                # The visit directory path.
                visit_dir = os.path.join(d, v)
                # Silently skip non-directories.
                if os.path.isdir(visit_dir):
                    # The visit (session) number.
                    sess_nbr = airc_coll.path2session_number(v)
                    # The XNAT session name.
                    sess = SESSION_FMT % (sbj, sess_nbr)
                    # If the session is not yet in XNAT, then yield the session and its files.
                    if xnat.get_session('QIN', sbj, sess).exists():
                        logger.debug("Skipping session %s since it has already been loaded to XNAT." % sess)
                    else:
                        # The DICOM file match pattern.
                        dpat = os.path.join(visit_dir, airc_coll.dicom_pattern)
                        # The visit directory DICOM file iterator.
                        dicom_file_iter = glob.iglob(dpat)
                        logger.debug("Discovered new session %s in %s" % (sess, visit_dir))
                        yield (sbj, sess, dicom_file_iter)

def group_dicom_files_by_series(*dicom_files):
    """
    Groups the given DICOM files by series.
    
    @param dicom_files: the DICOM files or directories
    @return: a series number => DICOM file names dictionary
    """
    ser_files_dict = {}
    for ds in iter_dicom_headers(*dicom_files):
        ser_files_dict.setdefault(int(ds.SeriesNumber), []).append(ds.filename)
    return ser_files_dict
