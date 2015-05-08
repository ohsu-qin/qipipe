# Absolute import (the default in a future Python release) resolves
# the collections import as the standard Python collections module
# rather than the staging collections module.
from __future__ import absolute_import

import os
import re
import glob
from collections import defaultdict
from functools import partial
from qiutil.logging import logger
import qixnat
import qidicom.hierarchy
from . import collections
from .roi import iter_roi
from .staging_error import StagingError


SUBJECT_FMT = '%s%03d'
"""The QIN subject name format with argument subject number."""

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
    """
    Aggregate with attributes :attr:`location`, :meth:`dicom` and :meth:`roi`.
    """

    def __init__(self, pattern, dicom_gen, roi_gen=None):
        """
        :param pattern: the scan file pattern
        :param dicom_gen: the :attr:`dicom` generator
        :param roi_gen: the :attr:`roi` generator
        """
        self.pattern = pattern
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


def iter_stage(project, collection, *inputs, **opts):
    """
    Iterates over the the new AIRC visits in the given input directories.
    This method is a staging generator which yields a tuple consisting
    of the subject, session, scan number and :class:`ScanIterators`
    object, e.g.::
    
        >> scan_input = next(iter_stage('QIN', 'Sarcoma', '/path/to/subject'))
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
    :attr:`qipipe.staging.collection.Collection.patterns`
    :attr:`qipipe.staging.collection.Patterns,subject`
    regular expression.

    Each iteration *(subject, session, scan, scan_iters)* tuple is
    formed as follows:

    - The *subject* is the XNAT subject name formatted by
      :data:`SUBJECT_FMT`.

    - The *session* is the XNAT experiment name formatted by
      :data:`SESSION_FMT`.

    - The *scan* is the XNAT scan number.

    - The *scan_iters* is the :class:`ScanIterators` object.

    :param project: the XNAT project name
    :param collection: the
        :attr:`qipipe.staging.collection.Collection.name`
    :param inputs: the source subject directories to stage
    :param opts: the following keyword option:
    :keyword scan: the scan number to stage
        (default stage all detected scans)
    :keyword skip_existing: flag indicating whether to ignore each
        existing session, or scan if the *scan* option is set
        (default True)
    :yield: the :class:`ScanInput` object
    """
    # Validate that there is a collection
    if not collection:
        raise StagingError('Staging is missing the AIRC collection name')
    
    # Group the new DICOM files into a
    # {subject: {session: {scan: scan iterators}} dictionary.
    stg_dict = _collect_visits(project, collection, *inputs, **opts)

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
                else:
                    _logger.debug("Skipping %s %s scan %d since no DICOM files"
                                  " were found for this scan matching %s." %
                                  (sbj, sess, scan, scan_iters.pattern))


def _collect_visits(project, collection, *inputs, **opts):
    """
    Collects the AIRC visits in the given input directories.
    The visit DICOM files are grouped by volume.

    :param project: the XNAT project name
    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories
    :param opts: the :meth:`iter_stage` options
    :return: the {subject: {session: {scan: :class:`ScanIterators`}}}
        dictionary
    """
    # The visit (subject, session, scan dictionary) tuple generator.
    visits = VisitIterator(project, collection, *inputs, **opts)

    # The dictionary to build.
    visit_dict = defaultdict(dict)
    # Add each tuple as a dictionary entry.
    for sbj, sess, scan_dict in visits:
        visit_dict[sbj][sess] = scan_dict
    
    return visit_dict


class VisitIterator(object):
    """Generator class for AIRC visits."""

    def __init__(self, project, collection, *subject_dirs, **opts):
        """
        :param project: the XNAT project name
        :param collection: the AIRC image collection name
        :param subject_dirs: the subject directories over which
            to iterate
        :param opts: the :meth:`iter_stage` options
        """
        self.project = project
        """The :meth:`iter_stage` project name parameter."""
        
        self.collection = collections.with_name(collection)
        """The :meth:`iter_stage` collection name parameter."""

        self.subject_dirs = subject_dirs
        """The input directories."""

        self.scan = opts.get('scan')
        """The :meth:`iter_stage` scan number option."""

        self.skip_existing = opts.get('skip_existing', True)
        """The :meth:`iter_stage` *skip_existing* flag option."""
        
        self.logger = logger(__name__)

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
        # The visit subdirectory matcher.
        vpat = self.collection.patterns.session
        # The {scan number: {dicom, roi}} file search patterns.
        all_scan_pats = self.collection.patterns.scan
        # The selected file search patterns.
        if self.scan:
            # Filter on only the specified scan.
            if self.scan not in all_scan_pats:
                raise StagingError("The %s scan %d is not supported" %
                                   (self.collection.name, self.scan))
            scan_pats = {self.scan: all_scan_pats[self.scan]}
        else:
            # Detect all scans.
            scan_pats = all_scan_pats

        filter_sess = self.skip_existing and not self.scan
        filter_scan = self.skip_existing and self.scan
        
        # Iterate over the visits.
        with qixnat.connect():
            for sbj_dir in self.subject_dirs:
                sbj_dir = os.path.abspath(sbj_dir)
                self.logger.debug("Discovering sessions in %s..." % sbj_dir)
                # Make the XNAT subject name.
                _, sbj_subdir = os.path.split(sbj_dir)
                sbj_nbr = self._match_subject_number(sbj_subdir)
                sbj = SUBJECT_FMT % (self.collection.name, sbj_nbr)
                # The subject subdirectory names which match the visit
                # pattern, sorted in visit order.
                sess_matches = sorted((d for d in os.listdir(sbj_dir)
                                       if vpat.match(d)))
                # Generate the new (subject, session, {scan: DICOM files})
                # tuples for each visit.
                for sess_subdir in sess_matches:
                    # The visit directory path.
                    sess_dir = os.path.join(sbj_dir, sess_subdir)
                    # Silently skip non-directories.
                    if not os.path.isdir(sess_dir):
                        continue
                    # The visit (session) number.
                    sess_nbr = self._match_session_number(sess_subdir)
                    # The XNAT session name.
                    sess = SESSION_FMT % sess_nbr
                    # Apply the selection filter, e.g. an XNAT existence
                    # check. If the session passes the filter, then the
                    # files qualify for iteration.
                    if not filter_sess or self._is_new_session(sbj, sess):
                        # The DICOM and ROI iterators for each scan number.
                        scan_dict = {scan: self._scan_iterators(pats, sess_dir)
                                     for scan, pats in scan_pats.iteritems()
                                     if not filter_scan or self._is_new_scan(sbj, sess, scan)}
                        if scan_dict:
                            scans = scan_dict.keys()
                            self.logger.debug("Discovered %s %s scans %s in %s" %
                                              (sbj, sess, scans, sess_dir))
                            yield sbj, sess, scan_dict
                        else:
                            self.logger.debug("No %s %s scans were discovered"
                                              " in %s" %  (sbj, sess, sess_dir))

    def _scan_iterators(self, patterns, base_dir):
        # The DICOM glob pattern.
        dcm_pat = os.path.join(base_dir, patterns.dicom)
        # The DICOM file generator.
        dcm_gen = _scan_dicom_generator(dcm_pat,
                                        self.collection.patterns.volume)
        # The ROI file match patterns.
        roi_pats = patterns.roi
        # Make the ROI generator, if necessary.
        if roi_pats:
            roi_gen = iter_roi(roi_pats.glob, roi_pats.regex, base_dir)
        else:
            roi_gen = None 
        
        return ScanIterators(pattern=dcm_pat, dicom_gen=dcm_gen, roi_gen=roi_gen)

    def _match_subject_number(self, path):
        """
        :param path: the directory path
        :return: the subject number
        :raise StagingError: if the path does not match the collection subject
            pattern
        """
        match = self.collection.patterns.subject.match(path)
        if not match:
            raise StagingError(
                "The directory path %s does not match the subject pattern %s" %
                (path, self.collection.patterns.subject.pattern))

        return int(match.group(1))

    def _match_session_number(self, path):
        """
        :param path: the directory path
        :return: the session number
        :raise StagingError: if the path does not match the collection session
            pattern
        """
        match = self.collection.patterns.session.match(path)
        if not match:
            raise StagingError(
                "The directory path %s does not match the session pattern %s" %
                (path, self.collection.patterns.session.pattern))
        return int(match.group(1))
    
    def _is_new_session(self, subject, session):
        with qixnat.connect() as xnat:
            exists = xnat.get_session(self.project, subject, session).exists()
        if exists:
            logger(__name__).debug("Skipping the %s %s %s since it has already"
                                   " been loaded to XNAT." % (subject, session))
        return not exists


    def _is_new_scan(self, subject, session, scan):
        with qixnat.connect() as xnat:
            exists = xnat.get_scan(self.project, subject, session, scan).exists()
        if exists:
            logger(__name__).debug("Skipping %s %s %s scan %d since it has"
                                   " already been loaded to XNAT." %
                                   (subject, session, scan))
        return not exists

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

        