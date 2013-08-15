"""Pipeline utility functions."""

import os, re, glob
from collections import defaultdict
from ..helpers.project import project
from ..helpers import xnat_helper
from ..helpers.dicom_helper import iter_dicom_headers
from .staging_error import StagingError
from . import airc_collection as airc

import logging
logger = logging.getLogger(__name__)

SUBJECT_FMT = '%s%03d'
"""The QIN subject name format with arguments (collection, subject number)."""

SESSION_FMT = 'Session%02d'
"""The QIN series name format with arguments (subject, series number)."""

def subject_for_directory(collection, path):
    """
    Infers the XNAT subject names from the given directory name.
    
    :param collection: the AIRC collection name
    :return: the corresponding XNAT subject label
    :raise StagingError: if the name does not match the collection subject pattern
    """
    airc_coll = airc.collection_with_name(collection)
    sbj_nbr = airc_coll.path2subject_number(path)
    return SUBJECT_FMT % (collection, sbj_nbr)
    
def get_subjects(collection, source, pattern=None):
    """
    Infers the XNAT subject names from the given source directory.
    The source directory contains subject subdirectories.
    The match pattern matches on the subdirectories and captures the
    subject number. The subject name is the collection name followed
    by the subject number, e.g. ``Breast004``.
    
    :param collection: the AIRC collection name
    :param source: the input parent directory
    :param pattern: the subject directory name match pattern (default
        is the :class:`qipipe.staging.airc_collection.AIRCCollection`
        ``subject_pattern``)
    :return: the subject name => directory dictionary
    """
    logger.debug("Detecting %s subjects from %s..." % (collection, source))
    airc_coll = airc.collection_with_name(collection)
    pat = pattern or airc_coll.subject_pattern
    sbj_dir_dict = {}
    for d in os.listdir(source):
        match = re.match(pat, d)
        if match:
            # The XNAT subject name.
            subject = SUBJECT_FMT % (collection, int(match.group(1)))
            # The subject source directory.
            sbj_dir_dict[subject] = os.path.join(source, d)
            logger.debug("Discovered XNAT test subject %s subdirectory: %s" % (subject, d))
    
    return sbj_dir_dict

def iter_new_visits(collection, *subject_dirs):
    """
    Iterates over the visits in the given subject directories which are not in XNAT.
    Each iteration item is a `(subject, session, files)` tuple, formed
    as follows:
    
    - The `subject` is the XNAT subject ID formatted by ``SUBJECT_FMT``
    
    - The `session` is the XNAT experiment name formatted by ``SESSION_FMT``
    
    - The `files` iterates over the files which match the
      :mod:`qipipe.staging.airc_collection` DICOM file include pattern
    
    The supported AIRC collections are defined by :mod:`qipipe.staging.airc_collection`.
    
    :param collection: the AIRC image collection name
    :param subject_dirs: the subject directories over which to iterate
    :return: the new visit `(subject, session, files)` tuples
    """
    return NewVisitIterator(collection, *subject_dirs)

def group_dicom_files_by_series(*dicom_files):
    """
    Groups the given DICOM files by series. Subtraction images, indicated by a ``SUB``
    DICOM Image Type, are ignored.
    
    :param dicom_files: the DICOM files or directories
    :return: a {series number: DICOM file names} dictionary
    """
    ser_files_dict = defaultdict(list)
    for ds in iter_dicom_headers(*dicom_files):
        # Ignore subtraction images.
        if not 'SUB' in ds.ImageType:
            ser_files_dict[int(ds.SeriesNumber)].append(ds.filename)
    
    return ser_files_dict

class NewVisitIterator(object):
    """
    **NewVisitIterator** is a generator class for detecting new AIRC visits.
    """

    def __init__(self, collection, *subject_dirs):
        """
        :param collection: the AIRC image collection name
        :param subject_dirs: the subject directories over which to iterate
        """
        # The AIRC collection with the given name.
        self.collection = airc.collection_with_name(collection)
        self.subject_dirs = subject_dirs
    
    def __iter__(self):
        return self.next()
    
    def next(self):
        """
        Iterates over the visits in the subject directories as described in
        :meth:`iter_new_visits`.
        """
        # The visit subdirectory match pattern.
        vpat = self.collection.session_pattern

        with xnat_helper.connection() as xnat:
            for d in self.subject_dirs:
                d = os.path.abspath(d)
                logger.debug("Discovering new sessions in %s..." % d)
                # Make the XNAT subject name.
                sbj_nbr = self.collection.path2subject_number(d)
                sbj = SUBJECT_FMT % (self.collection.name, sbj_nbr)
                # The subject subdirectories which match the visit pattern.
                vmatches = [v for v in os.listdir(d) if re.match(vpat, v)]
                # Generate the new (subject, session, DICOM files) items in each visit.
                for v in vmatches:
                    # The visit directory path.
                    visit_dir = os.path.join(d, v)
                    # Silently skip non-directories.
                    if os.path.isdir(visit_dir):
                        # The visit (session) number.
                        sess_nbr = self.collection.path2session_number(v)
                        # The XNAT session name.
                        sess = SESSION_FMT % sess_nbr
                        # If the session is not yet in XNAT, then yield the session and its files.
                        if xnat.get_session(project(), sbj, sess).exists():
                            logger.debug("Skipping the %s %s %s session"
                                " since it has already been loaded to XNAT." % (project(), sbj, sess))
                        else:
                            # The DICOM file match pattern.
                            dpat = os.path.join(visit_dir, self.collection.dicom_pattern)
                            # The visit directory DICOM file iterator.
                            dicom_file_iter = glob.iglob(dpat)
                            logger.debug("Discovered new session %s in %s" % (sess, visit_dir))
                            yield (sbj, sess, dicom_file_iter)
