"""Staging utility functions."""

import os
import re
import glob
from collections import defaultdict
from qiutil.logging import logger
import qixnat
from .. import project
import qidicom.hierarchy
from . import airc_collection as airc
from .roi import iter_roi
from .staging_error import StagingError


SUBJECT_FMT = '%s%03d'
"""The QIN subject name format with arguments (collection, subject number)."""

SESSION_FMT = 'Session%02d'
"""The QIN session name format with argument session number."""


class ScanInput(object):
    def __init__(self, collection, subject, session, scan, iterators):
        self.collection = collection
        """The image collection name."""

        self.subject = subject
        """The scan subject name."""
        
        self.session = session
        """The scan session name."""
        
        self.scan = scan
        """The scan number."""
        
        self.iterators = iterators
        """The :class:`ScanIterators` object."""

    def __repr__(self):
        return str(dict(collection=self.collection, subject=self.subject,
                        scan=self.scan, iterators=self.iterators))


class ScanIterators(object):
    """Aggregate with attributes :meth:`dicom` and :meth:`roi`."""

    def __init__(self, dicom_gen, roi_gen=None):
        """
        :param dicom_gen: the :attr:`dicom` generator
        :param roi_gen: the :attr:`roi` generator
        """
        self._dicom_gen = dicom_gen
        self._dicom = None
        self._roi_gen = roi_gen
        self._roi = None
    
    @property
    def dicom(self):
        """The {volume: [DICOM files]} dictionary."""
        if self._dicom == None:
            # The {volume: iterator} dictionary.
            dicom_iters = next(self._dicom_gen)
            # The {volume: list} dictionary.
            dicom_lists = {k: list(v) for k, v in dicom_iters.iteritems()}
            # Remove the empty file lists.
            self._dicom = {k: v for k, v in dicom_lists.iteritems() if v}
        return self._dicom
    
    @property
    def roi(self):
        """The :meth:`qipipe.staging.roi.iter_roi` iterator."""
        if self._roi == None:
            self._roi = list(self._roi_gen) if self._roi_gen else []
        return self._roi

    def __repr__(self):
        return str(dict(dicom=self.dicom, roi=self.roi))


def iter_stage(collection, *inputs, **opts):
    """
    Iterates over the the new AIRC visits in the given input directories.
    This method is a staging generator which yields a tuple consisting
    of the subject, session, scan number and :class:`ScanIterators`
    object, e.g.::
    
        >> scan_input = next(iter_stage('Sarcoma', '/path/to/subject'))
        >> print scan_input.subject
        Sarcoma001
        >> print scan_input.session
        Session01
        >> print scan_input.scan
        1
        >> print scan_input.iterators.dicom
        {1: ['/path/to/t1/image1.dcm', ...], ...}
        >> print scan_input.iterators.roi
        [(1, 19, '/path/to/roi19.bqf'), ...]

    The input directories conform to the
    :attr:`qipipe.staging.airc_collection.AIRCCollection.subject_pattern`

    :param collection: the
        :attr:`qipipe.staging.airc_collection.AIRCCollection.name`
    :param inputs: the AIRC source subject directories to stage
    :param opts: the following keyword option:
    :keyword skip_existing: flag indicating whether to ignore existing
        sessions (default True)
    :yield: the :class:`ScanInput` object
    """
    # Validate that there is a collection
    if not collection:
        raise ValueError('Staging is missing the AIRC collection name')
    
    # Group the new DICOM files into a
    # {subject: {session: {scan: scan iterators}} dictionary.
    stg_dict = _collect_visits(collection, *inputs, **opts)

    # Generate the (subject, session, :class:ScanIterators) tuples.
    _logger = logger(__name__)
    for sbj, sess_dict in stg_dict.iteritems():
        for sess, scan_dict in sess_dict.iteritems():
            for scan, scan_iters in scan_dict.iteritems():
                # The scan must have at least one DICOM file.
                if scan_iters.dicom:
                    _logger.debug("Staging %s session %s scan %d..." %
                                  (sbj, sess, scan))
                    yield ScanInput(collection, sbj, sess, scan, scan_iters)
                    _logger.debug("Staged %s session %s scan %d." %
                                  (sbj, sess, scan))


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
    :return: the {subject: {session: {scan: :class:`ScanIterators`}}}
        dictionary
    """
    # The visit (subject, session, scan dictionary) tuples.
    if opts.pop('skip_existing', True):
        visits = _detect_new_visits(collection, *inputs, **opts)
    else:
        visits = _iter_visits(collection, *inputs, **opts)

    # The dictionary to build.
    visit_dict = defaultdict(dict)
    # Add each tuple as a dictionary entry.
    for sbj, sess, scan_dict in visits:
        visit_dict[sbj][sess] = scan_dict
    
    return visit_dict


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
    Each iteration item is a *(subject, session, scan, scan_iters)* tuple,
    formed as follows:

    - The *subject* is the XNAT subject name formatted by
      :data:`SUBJECT_FMT`.

    - The *session* is the XNAT experiment name formatted by
      :data:`SESSION_FMT`.

    - The *scan* is the XNAT scan number.

    - The *scan_iters* is the :class:`ScanIterators` object.

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
        

class VisitIterator(object):
    """Generator class for AIRC visits."""

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
        
        :yield: the next (subject, session, scan_dict) tuple
        :yieldparam subject: the subject name
        :yieldparam session: the session name
        :yieldparam scan_dict: the {scan number: :class:`ScanIterators`}
            dictionary
        """
        # The visit subdirectory match pattern.
        vpat = self.collection.session_pattern
        logger(__name__).debug("The visit directory search pattern is %s..." %
                               vpat)
        
        # The {scan number: {dicom, roi}} file search patterns.
        scan_pats = self.collection.scan_patterns
        logger(__name__).debug("The scan file search pattern is %s..." %
                               scan_pats)
        
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
                # Generate the new (subject, session, scanDICOM files) items
                # in each visit.
                for sess_subdir in sess_matches:
                    # The visit directory path.
                    sess_dir = os.path.join(sbj_dir, sess_subdir)
                    # Silently skip non-directories.
                    if os.path.isdir(sess_dir):
                        # The visit (session) number.
                        sess_nbr = self.collection.path2session_number(sess_subdir)
                        # The XNAT session name.
                        sess = SESSION_FMT % sess_nbr
                        # Apply the selection filter, e.g. an XNAT existence
                        # check. If the session passes the filter, then the
                        # files qualify for iteration.
                        if self.filter(sbj, sess):
                            logger(__name__).debug("Discovered session %s in"
                                                   " %s" % (sess, sess_dir))
                            # The DICOM and ROI iterators for each scan number.
                            scan_dict = {scan: self._scan_iterators(pats, sess_dir)
                                         for scan, pats in scan_pats.iteritems()}
                            yield sbj, sess, scan_dict

    def _scan_iterators(self, patterns, base_dir):
        # The DICOM glob pattern.
        dcm_pat = os.path.join(base_dir, patterns.dicom)
        # The DICOM file generator.
        dcm_gen = _scan_dicom_generator(dcm_pat, self.collection.volume_tag)
        # The ROI file match patterns.
        roi_pats = patterns.roi
        # Make the ROI generator, if necessary.
        if roi_pats:
            roi_gen = iter_roi(roi_pats.glob, roi_pats.regex, base_dir)
        else:
            roi_gen = None 
        
        return ScanIterators(dicom_gen=dcm_gen, roi_gen=roi_gen)

    
def _scan_dicom_generator(pattern, tag):
    """
    :param pattern: the DICOM file glob pattern
    :param tag: the DICOM volume tag
    :yield: the {volume: [DICOM files]} dictionary 
    """
    # The visit directory DICOM file iterator.
    dicom_files = glob.iglob(pattern)
    
    # Group the DICOM files by volume.
    yield qidicom.hierarchy.group_by(tag, *dicom_files)

        