"""Pipeline utility functions."""

import os
import re
import glob
from collections import defaultdict
from qiutil.collections import nested_defaultdict
from qiutil.logging import logger
import qixnat
from .. import project
from qidicom import reader
from qidicom.hierarchy import group_by
from . import airc_collection as airc
from .staging_error import StagingError

SUBJECT_FMT = '%s%03d'
"""The QIN subject name format with arguments (collection, subject number)."""

SESSION_FMT = 'Session%02d'
"""The QIN session name format with argument session number."""


def iter_stage(collection, *inputs, **opts):
    """
    Iterates over the the new AIRC visits in the given input directories.
    This method is a staging generator which yields a tuple consisting
    of the subject, session and a {scan type: {scan number: [DICOM files]}}
    dictionary, e.g.::
    
        >> sbj, sess, scan_type_dict = next(iter_stage('Sarcoma', '/path/to/subject'))
        >> print sbj
        Sarcoma001
        >> print sess
        Session01
        >> print scan_type_dict
        {'t1': {20: ['/path/to/image1.dcm', ...]}, ...}

    The input directories conform to the
    :class:`qipipe.staging.airc_collection.AIRCCollection` *subject_pattern*)

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories to stage
    :param opts: the following keyword option:
    :keyword skip_existing: flag indicating whether to ignore existing
        sessions (default True)
    :yield: the DICOM files
    :yieldparam subject: the subject name
    :yieldparam session: the session name
    :yieldparam scan_type_dict: the *{scan type: {scan number: [DICOM files]}}*
        dictionary
    """
    # Validate that there is a collection
    if not collection:
        raise ValueError('Staging is missing the AIRC collection name')
    
    # Group the new DICOM files into a
    # {subject: {session: [(volume, dicom_files), ...]}} dictionary.
    stg_dict = _collect_visits(collection, *inputs, **opts)
    if not stg_dict:
        return

    # The workflow subjects.
    subjects = stg_dict.keys()

    # Print a debug message.
    volume_cnt = sum(map(len, stg_dict.itervalues()))
    logger(__name__).debug("Staging %d new %s volumes from %d subjects" %
                           (volume_cnt, collection, len(subjects)))
    
    # Generate the (subject, session, {scan type: {volume: [dicom files]}})
    # tuples.
    for sbj, sess_dict in stg_dict.iteritems():
        logger(__name__).debug("Staging subject %s..." % sbj)
        for sess, scan_type_dict in sess_dict.iteritems():
            logger(__name__).debug("Staging %s session %s..." % (sbj, sess))
            # Delegate to the workflow executor.
            yield sbj, sess, scan_type_dict
            logger(__name__).debug("Staged %s session %s." % (sbj, sess))
        logger(__name__).debug("Staged subject %s." % sbj)
    logger(__name__).debug("Staged %d new %s volumes from %d subjects." %
                           (volume_cnt, collection, len(subjects)))


def subject_for_directory(collection, path):
    """
    Infers the XNAT subject names from the given directory name.

    :param collection: the AIRC collection name
    :return: the corresponding XNAT subject label
    :raise StagingError: if the name does not match the collection subject
        pattern
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
        *subject_pattern*)
    :return: the subject name => directory dictionary
    """
    logger(__name__).debug("Detecting %s subjects from %s..." %
          (collection, source))
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
            logger(__name__).debug(
                "Discovered XNAT test subject %s subdirectory: %s" %
                (subject, d))

    return sbj_dir_dict


def _collect_visits(collection, *inputs, **opts):
    """
    Collects the AIRC visits in the given input directories.
    The visit DICOM files are grouped by volume.

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories
    :param opts: the :class:`VisitIterator` initializer options,
        as well as the following keyword option:
    :keyword skip_existing: flag indicating whether to ignore existing
        sessions (default True)
    :return: the *{subject: {session: {scan type: [dicom files]}}}*
        dictionary
    """
    if opts.pop('skip_existing', True):
        visits = _detect_new_visits(collection, *inputs, **opts)
    else:
        visits = list(_iter_visits(collection, *inputs, **opts))

    # Group the DICOM files by volume.
    return _group_visits(*visits)


def _detect_new_visits(collection, *inputs, **opts):
    """
    Detects the new AIRC visits in the given input directories. The visit
    DICOM files are grouped by volume.

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories
    :param opts: the :class:`VisitIterator` initializer options
    :return: the *{subject: {session: {volume: [dicom files]}}}* dictionary
    """
    # Collect the AIRC visits into (subject, session, dicom_files)
    # tuples.
    visits = list(_iter_new_visits(collection, *inputs, **opts))

    # If no images were detected, then bail.
    if not visits:
        logger(__name__).info("No new visits were detected in the input"
                              " directories.")
        return {}
    logger(__name__).debug("%d new visits were detected" % len(visits))

    # Group the DICOM files by volume.
    return visits


def _iter_visits(collection, *inputs, **opts):
    """
    Iterates over the visits in the given subject directories.
    Each iteration item is a *(subject, session, volume_dict)* tuple,
    formed as follows:

    - The *subject* is the XNAT subject name formatted by
      :data:`SUBJECT_FMT`.

    - The *session* is the XNAT experiment name formatted by
      :data:`SESSION_FMT`.

    - The *volume_dict* generator iterates over the files which match the
      :mod:`qipipe.staging.airc_collection` DICOM file include pattern,
      grouped by the scan type.

    The supported AIRC collections are defined by
    :mod:`qipipe.staging.airc_collection`.

    :param collection: the AIRC image collection name
    :param inputs: the subject directories over which to iterate
    :param opts: the :class:`VisitIterator` initializer options
    :yield: iterate over the visit *(subject, session, files)* tuples
    """
    return VisitIterator(collection, *inputs, **opts)


def _iter_new_visits(collection, *inputs, **opts):
    """
    Filters :meth:`qipipe.staging.staging_helper._iter_visits` to iterate
    over the new visits in the given subject directories which are not in XNAT.

    :param collection: the AIRC image collection name
    :param inputs: the subject directories over which to iterate
    :param opts: the :meth:`_iter_visits` options
    :yield: iterate over the new visit *(subject, session, volume_dict)* tuples
    """
    opts['filter'] = _is_new_session
    return _iter_visits(collection, *inputs, **opts)


def _is_new_session(subject, session):
    # If the session is not yet in XNAT, then yield the tuple.
    with qixnat.connect() as xnat:
        exists = xnat.get_session(project(), subject, session).exists()
    if exists:
        logger(__name__).debug("Skipping the %s %s %s session since it has"
                               " already been loaded to XNAT." %
                               (project(), subject, session))

    return not exists


def _group_visits(*visit_tuples):
    """
    Creates the staging dictionary for the new images in the given
    visit tuples.

    :param visit_tuples: the *(subject, session, {scan type: [dicom files]})*
        tuples to group
    :return: the *{subject: {session: {scan type: {volume: [dicom files]}}}}*
        dictionary
    """
    # The {subject: {session: {volume: {scan_type: [dicom files]}}}} output.
    stg_dict = nested_defaultdict(dict, 3)
    for sbj, sess, scan_type_dict in visit_tuples:
        # Group the session DICOM input files by volume within scan type.
        for scan_type, dcm_iter in scan_type_dict.iteritems():
            volume_dict = group_by('SeriesNumber', *dcm_iter)
            for volume, dcm_iter in volume_dict.iteritems():
                stg_dict[sbj][sess][scan_type][volume] = dcm_iter

    return stg_dict


class VisitIterator(object):

    """
    **VisitIterator** is a generator class for AIRC visits.
    """

    def __init__(self, collection, *subject_dirs, **opts):
        """
        :param collection: the AIRC image collection name
        :param subject_dirs: the subject directories over which to iterate
        :param opts: the following initialization options:
        :keyword filter: a *(subject, session)* selection filter
        """
        self.collection = airc.collection_with_name(collection)
        """The AIRC collection with the given name."""

        self.subject_dirs = subject_dirs
        """The input directories."""

        self.filter = opts.get('filter', lambda subject, session: True)
        """The (subject, session) selection filter."""

    def __iter__(self):
        return self.next()

    def next(self):
        """
        Iterates over the visits in the subject directories.
        
        :yield: the next (subject, session, {scan type: DICOM files}) tuple
        """
        # The visit subdirectory match pattern.
        vpat = self.collection.session_pattern
        logger(__name__).debug("The visit directory search pattern is %s..." %
                               vpath)
        
        # The DICOM file search pattern depends on the scan type.
        dcm_pat_dict = self.collection.dcm_pat_dict
        logger(__name__).debug("The DICOM file search pattern is %s..." %
                               dcm_pat_dict)
        
        # Iterate over the visits.
        with qixnat.connect():
            for sbj_dir in self.subject_dirs:
                sbj_dir = os.path.abspath(sbj_dir)
                logger(__name__).debug("Discovering sessions in %s..." %
                                       sbj_dir)
                
                # Make the XNAT subject name.
                sbj_nbr = self.collection.path2subject_number(sbj_dir)
                sbj = SUBJECT_FMT % (self.collection.name, sbj_nbr)
                # The subject subdirectories which match the visit pattern.
                sess_matches = [subdir for subdir in os.listdir(sbj_dir)
                                if re.match(vpat, subdir)]
                
                # Generate the new (subject, session, DICOM files) items in
                # each visit.
                for sess_subdir in sess_matches:
                    # The visit directory path.
                    sess_dir = os.path.join(sbj_dir, sess_subdir)
                    # Silently skip non-directories.
                    if os.path.isdir(sess_dir):
                        # The visit (session) number.
                        sess_nbr = self.collection.path2session_number(
                            sess_subdir)
                        # The XNAT session name.
                        sess = SESSION_FMT % sess_nbr
                        
                        # Apply the selection filter, e.g. an XNAT existence
                        # check. If the session passes the filter, then
                        # the files qualify for iteration.
                        if self.filter(sbj, sess):
                            logger(__name__).debug("Discovered session %s in"
                                                   " %s" % (sess, sess_dir))
                            # The DICOM file match patterns.
                            scan_type_dict = {}
                            for scan_type, dcm_pat in dcm_pat_dict.iteritems():
                                file_pat = os.path.join(sess_dir, dcm_pat)
                                # The visit directory DICOM file iterator.
                                dcm_file_iter = glob.iglob(file_pat)
                                scan_type_dict[scan_type] = dcm_file_iter
                            yield (sbj, sess, scan_type_dict)
