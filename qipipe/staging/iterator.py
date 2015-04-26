"""Staging utility functions."""

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
from .roi import iter_roi
from .staging_error import StagingError

SUBJECT_FMT = '%s%03d'
"""The QIN subject name format with arguments (collection, subject number)."""

SESSION_FMT = 'Session%02d'
"""The QIN session name format with argument session number."""
        

def iter_stage(collection, *inputs, **opts):
    """
    Iterates over the the new AIRC visits in the given input directories.
    This method is a staging generator which yields a tuple consisting
    of the subject, session, scan number and {volume number: [dicom files]}
    dictionary, e.g.::
    
        >> sbj, sess, scan, vol_dcm_dict, rois =
        ...    next(iter_stage('Sarcoma', '/path/to/subject'))
        >> print sbj
        Sarcoma001
        >> print sess
        Session01
        >> print scan
        1
        >> print vol_dcm_dict
        {1: ['/path/to/t1/image1.dcm', ...], ...}
        >> print rois
        [(1, 19, '/path/to/roi19.bqf'), ...]

    The input directories conform to the
    :attr:`qipipe.staging.airc_collection.AIRCCollection.subject_pattern`

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories to stage
    :param opts: the following keyword option:
    :keyword skip_existing: flag indicating whether to ignore existing
        sessions (default True)
    :yield: the DICOM files
    :yieldparam subject: the subject name
    :yieldparam session: the session name
    :yieldparam scan: the scan number
    :yieldparam vol_dcm_dict: the {volume number: [DICOM files]} dictionary
    :yieldparam rois: the :meth:`qipipe.staging.roi.iter_roi` tuples
    """
    # Validate that there is a collection
    if not collection:
        raise ValueError('Staging is missing the AIRC collection name')
    
    # Group the new DICOM files into a
    # {subject: {session: {scan: [(volume, [files]), ...]}}} dictionary.
    stg_dict = _collect_visits(collection, *inputs, **opts)
    if not stg_dict:
        return

    # The workflow subjects.
    subjects = stg_dict.keys()

    # Print a debug message.
    volume_cnt = sum(map(len, stg_dict.itervalues()))
    logger(__name__).debug("Staging %d new %s volumes from %d subjects" %
                           (volume_cnt, collection, len(subjects)))
    
    # Generate the (subject, session, scan number, {volume: [dicom files]})
    # tuples.
    for sbj, sess_dict in stg_dict.iteritems():
        logger(__name__).debug("Staging subject %s..." % sbj)
        for sess, scan_dict in sess_dict.iteritems():
            for scan, vol_dcm_dict, rois in scan_dict.iteritems():
                logger(__name__).debug("Staging %s session %s scan %d..." %
                                       (sbj, sess, scan))
                # Delegate to the workflow executor.
                yield sbj, sess, scan, vol_dcm_dict, rois
                logger(__name__).debug("Staged %s session %s scan %d." %
                                       (sbj, sess, scan))
        logger(__name__).debug("Staged subject %s." % sbj)
    logger(__name__).debug("Staged %d new %s volumes from %d subjects." %
                           (volume_cnt, collection, len(subjects)))


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
    :return: the {subject: {session: {scan: ({volume: [dicom files]}, rois)}}}
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
    DICOM files are grouped by volume within scan within session within
    subject.

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories
    :param opts: the :class:`VisitIterator` initializer options
    :return: the :meth:`iter_new_visits` tuples
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
    Each iteration item is a *(subject, session, scan, vol_dict)* tuple,
    formed as follows:

    - The *subject* is the XNAT subject name formatted by
      :data:`SUBJECT_FMT`.

    - The *session* is the XNAT experiment name formatted by
      :data:`SESSION_FMT`.

    - The *scan* is the XNAT scan number.

    - The *vol_dict* generator iterates over the files which match the
      :mod:`qipipe.staging.airc_collection` DICOM file include pattern,
      grouped by the volume number.

    The supported AIRC collections are defined by
    :mod:`qipipe.staging.airc_collection`.

    :param collection: the AIRC image collection name
    :param inputs: the subject directories over which to iterate
    :param opts: the :class:`VisitIterator` initializer options
    :yield: the :class:`VisitIterator.next` tuple
    """
    return VisitIterator(collection, *inputs, **opts)


def _iter_new_visits(collection, *inputs, **opts):
    """
    Filters :meth:`qipipe.staging.iterator._iter_visits` to iterate over
    the new visits in the given subject directories which are not in XNAT.

    :param collection: the AIRC image collection name
    :param inputs: the subject directories over which to iterate
    :param opts: the :meth:`_iter_visits` options
    :yield: the :meth:`_iter_visits` tuple
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
    Creates the staging dictionary for the images in the given visit tuples.
    The input tuples are grouped into a dictionary expressing the hierarchy
    *subject*/*session*/*scan*/*volume*/[DICOM files]. The 2D DICOM files
    are grouped by ``AcquisitionNumber``.
    
    TODO - get the grouping criterion from airc_collection.

    :param visit_tuples: the :meth:`_iter_visits` tuples to group
    :return: the *{subject: {session: {scan: {volume: [dicom files]}}}}*
        dictionary
    """
    # The dictionary to build.
    stg_dict = nested_defaultdict(dict, 3)
    # Add each tuple as a dictionary entry.
    for sbj, sess, scan_dict in visit_tuples:
        # Group the session DICOM input files by volume within scan type.
        for scan, dcm_iter in scan_dict.iteritems():
            vol_dcm_dict = group_by('AcquisitionNumber', *dcm_iter)
            for volume, dcm_iter in vol_dcm_dict.iteritems():
                stg_dict[sbj][sess][scan][volume] = dcm_iter

    return stg_dict


class VisitIterator(object):

    """
    **VisitIterator** is a generator class for AIRC visits.
    """

    def __init__(self, collection, *subject_dirs, **opts):
        """
        :param collection: the AIRC image collection name
        :param subject_dirs: the subject directories over which
            to iterate
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
        
        :yield: the next (subject, session, scan_dict, rois) tuple
        :yieldparam subject: the subject name
        :yieldparam session: the session name
        :yieldparam scan_dict: the {scan number: [DICOM files]} dictionary
        :yieldparam rois: the :meth:`qipipe.staging.roi.iter_roi` iterator
        """
        # The visit subdirectory match pattern.
        vpat = self.collection.session_pattern
        logger(__name__).debug("The visit directory search pattern is %s..." %
                               vpat)
        
        # The DICOM file search pattern depends on the scan type.
        dcm_dict = self.collection.scan_dicom_patterns
        logger(__name__).debug("The DICOM file search pattern is %s..." %
                               dcm_dict)
        
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
                # Generate the new (subject, session, scanDICOM files) items in
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
                            scan_dict = {}
                            for scan, dcm_pat in dcm_dict.iteritems():
                                file_pat = os.path.join(sess_dir, dcm_pat)
                                # The visit directory DICOM file iterator.
                                dcm_file_iter = glob.iglob(file_pat)
                                scan_dict[scan] = dcm_file_iter
                            
                            # The ROI file match pattern.
                            roi_iter = iter_roi(self.collection.name, sess_dir)
                            yield (sbj, sess, scan_dict, roi_iter)