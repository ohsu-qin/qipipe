# Absolute import (the default in a future Python release) resolves
# the collections import as the standard Python collections module
# rather than the staging collections module.
from __future__ import absolute_import
import os
import re
import glob
from bunch import Bunch
from collections import defaultdict
from ..helpers.logging import logger
import qixnat
import qidicom.hierarchy
from .. import staging
from ..helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from . import image_collection
from .roi import iter_roi
from .staging_error import StagingError


def iter_stage(project, collection, *inputs, **opts):
    """
    Iterates over the the scans in the given input directories.
    This method is a staging generator which yields a tuple consisting
    of the {subject, session, scan, dicom, roi} object, e.g.::

    The input directories conform to the
    :attr:`qipipe.staging.image_collection.Collection.patterns`
    ``subject`` regular expression.

    Each iteration {subject, session, scan, dicom, roi} object
    is formed as follows:

    - The *subject* is the XNAT subject name formatted by
      :data:`SUBJECT_FMT`.

    - The *session* is the XNAT experiment name formatted by
      :data:`SESSION_FMT`.

    - The *scan* is the XNAT scan number.

    - *dicom* is the DICOM directory.

    - *roi* is the ROI directory.

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
    :yield: the {subject, session, scan, dicom, roi} objects
    """
    # Validate that there is a collection.
    if not collection:
        raise StagingError('Staging is missing the image collection name')

    # Group the new DICOM files into a
    # {subject: {session: {scan: scan iterators}} dictionary.
    stg_dict = _collect_visits(project, collection, *inputs, **opts)

    # Generate the {subject, session, scan} objects.
    _logger = logger(__name__)
    for sbj, sess_dict in stg_dict.iteritems():
        for sess, scan_dict in sess_dict.iteritems():
            for scan, scan_dirs in scan_dict.iteritems():
                # The scan must have DICOM files.
                if scan_dirs.dicom:
                    _logger.debug("Staging %s %s scan %d..." % (sbj, sess, scan))
                    yield Bunch(subject=sbj, session=sess, scan=scan, **scan_dirs)
                    _logger.info("Staged %s %s scan %d." % (sbj, sess, scan))
                else:
                    _logger.info("Skipping %s %s scan %d since no DICOM files"
                                  " were found for this scan." %
                                  (sbj, sess, scan))


def _collect_visits(project, collection, *inputs, **opts):
    """
    Collects the sessions in the given input directories.

    :param project: the XNAT project name
    :param collection: the TCIA image collection name
    :param inputs: the source DICOM subject directories
    :param opts: the :meth:`iter_stage` options
    :return: the {subject: {session: {scan: {dicom, roi}}}}
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

        self.collection = image_collection.with_name(collection)
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
        * *scan_dict* is the scan {number: {dicom, roi}}
            dictionary

        :return: the next (subject, session, scan_dict) tuple
        """
        # The visit subdirectory matcher.
        vpat = self.collection.patterns.session
        # The {scan number: {dicom, roi}} directory search patterns.
        all_scan_pats = self.collection.patterns.scan
        # The selected directory search patterns.
        if self.scan:
            # Filter on only the specified scan.
            if self.scan not in all_scan_pats:
                raise StagingError("The %s scan %d is not supported"
                                   " with an image collection DICOM"
                                   " pattern" %
                                   (self.collection.name, self.scan))
            scan_pats = {self.scan: all_scan_pats[self.scan]}
        else:
            # Detect all scans.
            scan_pats = all_scan_pats

        # Filter existing scans if the skip_existing flag and scan
        # number are set.
        filter_scan = self.skip_existing and self.scan
        # Skip all scans of an existing session if the skip_existing
        # flag is set and the scan number is not set.
        skip_existing_session = self.skip_existing and not self.scan

        # Iterate over the visits.
        with qixnat.connect():
            # Generate the new (subject, session, {scan: directory})
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
                                      " in %s." % (sbj, sess, sess_dir))
                    continue
                # The DICOM and ROI directories for each scan number.
                scan_dict = {}
                for scan, pats in scan_pats.iteritems():
                    if not filter_scan or self._is_new_scan(sbj, sess, scan):
                        scan_dirs = self._scan_directories(pats, sess_dir)
                        if scan_dirs:
                            scan_dict[scan] = scan_dirs
                if scan_dict:
                    scans = scan_dict.keys()
                    self.logger.info("Discovered %s %s scans %s in %s." %
                                      (sbj, sess, scans, sess_dir))
                    yield sbj, sess, scan_dict
                else:
                    self.logger.info("No %s %s scans were discovered"
                                      " in %s." % (sbj, sess, sess_dir))

    def _scan_directories(self, patterns, input_dir):
        # The DICOM directory pattern.
        dcm_pat = "%s/%s" % (input_dir, patterns.dicom)
        # The DICOM directory matches.
        dcm_dirs = glob.glob(dcm_pat)
        # If no DICOM directory, then the scan will be ignored.
        if dcm_dirs:
            self.logger.debug("Discovered DICOM directories %s." % dcm_dirs)
        else:
            dcm_dirs = None
            self.logger.debug("No directory matches the DICOM pattern %s." %
                              dcm_pat)

        # The ROI directory is optional.
        roi_dirs = []
        # The ROI glob pattern.
        if hasattr(patterns, 'roi'):
            # The ROI directory pattern.
            roi_pat = "%s/%s" % (input_dir, patterns.roi.glob)
            # The ROI directory matches.
            roi_dirs = glob.glob(roi_pat)
            if roi_dirs:
                self.logger.debug("Discovered %d ROI directories." %
                                  len(roi_dirs))
            else:
                self.logger.debug("No directory was found matching the"
                                  " ROI pattern %s." % roi_pat)

        return Bunch(dicom=dcm_dirs, roi=roi_dirs)

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
                "The directory path %s does not match the subject pattern %s." %
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
                "The directory path %s does not match the session pattern %s." %
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
