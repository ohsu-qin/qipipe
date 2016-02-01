# Absolute import (the default in a future Python release) resolves
# the collections import as the standard Python collections module
# rather than the staging collections module.
from __future__ import absolute_import
import os
import re
import glob
from collections import defaultdict
from qiutil.logging import logger
import qixnat
import qidicom.hierarchy
from .. import staging
from ..helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from .roi import iter_roi
from .staging_error import StagingError


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
        return (self.__class__.__name__ +
                str(dict(collection=self.collection, subject=self.subject,
                         scan=self.scan, iterators=self.iterators)))


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
            if self._roi_gen:
                self._roi = list(self._roi_gen)
                logger(__name__).debug("Found %d ROI files." %
                                       len(self._roi))
            else:
                self._roi = []
        
        return self._roi
    
    def __repr__(self):
        return (self.__class__.__name__ +
                str(dict(dicom=self.dicom, roi=self.roi)))


def iter_stage(project, collection, *inputs, **opts):
    """
    Iterates over the the scans in the given input directories.
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
    :attr:`qipipe.staging.image_collection.Collection.patterns`
    :attr:`qipipe.staging.image_collection.Patterns.subject`
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
        :attr:`qipipe.staging.image_collection.Collection.name`
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
        raise StagingError('Staging is missing the image collection name')
    
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
                    _logger.debug("Staging %s %s scan %d..." % (sbj, sess, scan))
                    yield ScanInput(collection, sbj, sess, scan, scan_iters)
                    _logger.debug("Staged %s %s scan %d." % (sbj, sess, scan))
                else:
                    _logger.debug("Skipping %s %s scan %d since no DICOM files"
                                  " were found for this scan matching %s." %
                                  (sbj, sess, scan, scan_iters.pattern))


def _collect_visits(project, collection, *inputs, **opts):
    """
    Collects the sessions in the given input directories. The session
    DICOM files are grouped by volume.
    
    :param project: the XNAT project name
    :param collection: the TCIA image collection name
    :param inputs: the source DICOM subject directories
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
    """Scan DICOM generator class ."""
    
    def __init__(self, project, collection, *session_dirs, **opts):
        """
        :param project: the XNAT project name
        :param collection: the image collection name
        :param session_dirs: the session directories over which
            to iterate
        :param opts: the :meth:`iter_stage` options
        """
        self.project = project
        """The :meth:`iter_stage` project name parameter."""
        
        self.collection = staging.image_collection.with_name(collection)
        """The :meth:`iter_stage` collection name parameter."""
        
        self.session_dirs = session_dirs
        """The input directories."""
        
        self.scan = opts.get('scan')
        """The :meth:`iter_stage` scan number option."""
        
        self.skip_existing = opts.get('skip_existing', True)
        """The :meth:`iter_stage` *skip_existing* flag option."""
        
        self.logger = logger(__name__)
    
    def __iter__(self):
        """
        Returns the next (subject, session, scan_dict) tuple for the
        scans in the session directories, where:
        * *subject* is the subject name
        * *session* is the session name
        * *scan_dict* is the {scan number: :class:`ScanIterators`}
            dictionary
        
        :return: the next (subject, session, scan_dict) tuple
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
        
        # Filter existing scans if the skip_existing flag and scan
        # number are set.
        filter_scan = self.skip_existing and self.scan
        # Skip all scans of an existiong session if the skip_existing
        # flag is set and the scan number is not set.
        skip_existing_session = self.skip_existing and not self.scan
        
        # Iterate over the visits.
        with qixnat.connect():
            # Generate the new (subject, session, {scan: DICOM files})
            # tuples for each visit.
            for input_dir in self.session_dirs:
                sess_dir = os.path.abspath(input_dir)
                self.logger.debug("Discovering scans in %s..." % sess_dir)
                # The input directory is /path/to/<subject>/<visit>.
                sbj_dir, sess_basename = os.path.split(sess_dir)
                _, sbj_basename = os.path.split(sbj_dir)
                sbj_nbr = self._match_subject_number(sbj_basename)
                # Make the XNAT subject name.
                sbj = SUBJECT_FMT % (self.collection.name, sbj_nbr)
                # The visit (session) number.
                sess_nbr = self._match_session_number(sess_basename)
                # The XNAT session name.
                sess = SESSION_FMT % sess_nbr
                if skip_existing_session and not self._is_new_session(sbj, sess):
                    self.logger.debug("Skipping the existing %s %s session"
                                      " in %s" %  (sbj, sess, sess_dir))
                    continue
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
    
    def _scan_iterators(self, patterns, input_dir):
        # The DICOM glob pattern.
        dcm_pat = os.path.join(input_dir, patterns.dicom)
        # The DICOM file generator.
        dcm_gen = _scan_dicom_generator(dcm_pat,
                                        self.collection.patterns.volume)
        # The ROI file match patterns.
        roi_pats = patterns.roi
        # Make the ROI generator, if necessary.
        if roi_pats:
            roi_gen = iter_roi(roi_pats.glob, roi_pats.regex, input_dir)
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
            sess = xnat.find_one(self.project, subject, session)
        if sess:
            logger(__name__).debug("Skipping %s %s since it has already been"
                                   " loaded to XNAT." % (subject, session))
        return not sess
    
    def _is_new_scan(self, subject, session, scan):
        with qixnat.connect() as xnat:
            scan_obj = xnat.find_one(self.project, subject, session, scan=scan)
        if scan_obj:
            logger(__name__).debug("Skipping %s %s scan %d since it has"
                                   " already been loaded to XNAT." %
                                   (subject, session, scan))
        return not scan_obj


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
        
        