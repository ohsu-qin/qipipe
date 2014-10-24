"""Pipeline utility functions."""

import os
import re
import glob
from collections import defaultdict
from .. import project
from qiutil import xnat_helper
from qiutil.dicom_helper import iter_dicom_headers
from qiutil.logging_helper import logger
from . import airc_collection as airc
from .map_ctp import map_ctp

SUBJECT_FMT = '%s%03d'
"""The QIN subject name format with arguments (collection, subject number)."""

SESSION_FMT = 'Session%02d'
"""The QIN series name format with arguments (subject, series number)."""


def iter_stage(collection, *inputs, **opts):
    """
    Iterates over the the new AIRC visits in the given input directories.
    This method is a staging generator which yields the DICOM files for
    each new visit. After iteration is completed, the TCIA subject map
    is created in the given destination directory.

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories to stage
    :param opts: the the :meth:`collect_visits` options and the following
        keyword option:
    :keyword dest: the TCIA staging destination directory (default is
        the current working directory)
    :yield: stage the DICOM files
    :yieldparam subject: the subject name
    :yieldparam session: the session name
    :yieldparam ser_dicom_dict: the *{series: [dicom files]}* dictionary
    """
    # Validate that there is a collection
    if not collection:
        raise ValueError('Staging is missing the AIRC collection name')

    # The staging location.
    dest = opts.pop('dest', None)
    if dest:
        dest = os.path.abspath(dest)
    else:
        dest = os.getcwd()
    
    # Group the new DICOM files into a
    # {subject: {session: [(series, dicom_files), ...]}} dictionary.
    stg_dict = collect_visits(collection, *inputs, **opts)
    if not stg_dict:
        return

    # The workflow subjects.
    subjects = stg_dict.keys()

    # Print a debug message.
    series_cnt = sum(map(len, stg_dict.itervalues()))
    logger(__name__).debug("Staging %d new %s series from %d subjects in"
                           " %s..." % (series_cnt, collection, len(subjects),
                                       dest))
    
    # Delegate to the input function for each session.
    for sbj, sess_dict in stg_dict.iteritems():
        logger(__name__).debug("Staging subject %s..." % sbj)
        for sess, ser_dicom_dict in sess_dict.iteritems():
            logger(__name__).debug("Staging %s session %s..." % (sbj, sess))
            # Delegate to the workflow executor.
            yield sbj, sess, ser_dicom_dict
            logger(__name__).debug("Staged %s session %s." % (sbj, sess))
        logger(__name__).debug("Staged subject %s." % sbj)
    logger(__name__).debug("Staged %d new %s series from %d subjects in %s." %
                     (series_cnt, collection, len(subjects), dest))
    
    # Make the TCIA subject map.
    map_ctp(collection, *subjects, dest=dest)


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


def group_dicom_files_by_series(*dicom_files):
    """
    Groups the given DICOM files by series. Subtraction images, indicated by
    a ``SUB`` DICOM Image Type, are ignored.

    :param dicom_files: the DICOM files or directories
    :return: a {series number: DICOM file names} dictionary
    """
    ser_files_dict = defaultdict(list)
    for ds in iter_dicom_headers(*dicom_files):
        # Ignore subtraction images.
        if not 'SUB' in ds.ImageType:
            ser_files_dict[int(ds.SeriesNumber)].append(ds.filename)

    return ser_files_dict


def collect_visits(collection, *inputs, **opts):
    """
    Collects the AIRC visits in the given input directories.
    The visit DICOM files are grouped by series.

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories
    :param opts: the following keyword option:
    :keyword scan_type: the ``dce`` or ``t2`` scan type
        (default ``dce``)
    :keyword resume: flag indicating whether to
        forego checking for existing sessions (default False)
    :return: the *{subject: {session: {series: [dicom files]}}}*
        dictionary
    """
    if opts.pop('resume', False):
        visits = list(_iter_visits(collection, *inputs, **opts))
    else:
        visits = _detect_new_visits(collection, *inputs, **opts)

    # Group the DICOM files by series.
    return _group_sessions_by_series(*visits)


def _detect_new_visits(collection, *inputs, **opts):
    """
    Detects the new AIRC visits in the given input directories. The visit
    DICOM files are grouped by series.

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories
    :param opts: the following keyword option:
    :keyword scan_type: the ``dce`` or ``t2`` scan type
        (default ``dce``)
    :return: the *{subject: {session: {series: [dicom files]}}}* dictionary
    """
    # Collect the AIRC visits into (subject, session, dicom_files)
    # tuples.
    visits = list(iter_new_visits(collection, *inputs, **opts))

    # If no images were detected, then bail.
    if not visits:
        logger(__name__).info("No new visits were detected in the input"
                              " directories.")
        return {}
    logger(__name__).debug("%d new visits were detected" % len(visits))

    # Group the DICOM files by series.
    return visits


def _iter_visits(collection, *inputs, **opts):
    """
    Iterates over the visits in the given subject directories.
    Each iteration item is a *(subject, session, files)* tuple, formed
    as follows:

    - The *subject* is the XNAT subject name formatted by
      :data:`SUBJECT_FMT`

    - The *session* is the XNAT experiment name formatted by
      :data:`SESSION_FMT`

    - The *files* generator iterates over the files which match the
      :mod:`qipipe.staging.airc_collection` DICOM file include pattern

    The supported AIRC collections are defined by
    :mod:`qipipe.staging.airc_collection`.

    :param collection: the AIRC image collection name
    :param inputs: the subject directories over which to iterate
    :param opts: the following keyword arguments:
    :keyword filter: a *(subject, session)* selection filter
    :keyword scan_type: the ``dce`` or ``t2`` scan type
        (default ``dce``)
    :yield: iterate over the visit *(subject, session, files)* tuples
    """
    return VisitIterator(collection, *inputs, **opts)


def iter_new_visits(collection, *inputs, **opts):
    """
    Filters :meth:`qipipe.staging.staging_helper._iter_visits` to iterate
    over the new visits in the given subject directories which are not in XNAT.

    :param collection: the AIRC image collection name
    :param inputs: the subject directories over which to iterate
    :param opts: the :meth:`_iter_visits` options
    :yield: iterate over the new visit *(subject, session, files)* tuples
    """
    opts['filter'] = _is_new_session
    return _iter_visits(collection, *inputs, **opts)


def _is_new_session(subject, session):
    # If the session is not yet in XNAT, then yield the tuple.
    with xnat_helper.connection() as xnat:
        exists = xnat.get_session(project(), subject, session).exists()
    if exists:
        logger(__name__).debug("Skipping the %s %s %s session since it has"
                               " already been loaded to XNAT." %
                               (project(), subject, session))

    return not exists


def _group_sessions_by_series(*session_specs):
    """
    Creates the staging dictionary for the new images in the given
    sessions.

    :param session_specs: the *(subject, session, dicom_files)* tuples
        to group
    :return: the *{subject: {session: {series: [dicom files]}}}*
        dictionary
    """

    # The {subject: {session: {series: [dicom files]}}} output.
    stg_dict = defaultdict(dict)

    for sbj, sess, dcm_file_iter in session_specs:
        # Group the session DICOM input files by series.
        ser_dcm_dict = group_dicom_files_by_series(dcm_file_iter)
        if not ser_dcm_dict:
            raise StagingError("No DICOM files were detected in the "
                               "%s %s session source directory." %
                               (sbj, sess))
        # Collect the (series, dicom_files) tuples.
        stg_dict[sbj][sess] = ser_dcm_dict

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
        :keyword scan_type: the ``dce`` or ``t2`` scan type
            (default ``dce``)
        """
        self.collection = airc.collection_with_name(collection)
        """The AIRC collection with the given name."""

        self.subject_dirs = subject_dirs
        """The input directories."""

        self.filter = opts.get('filter', lambda subject, session: True)
        """The (subject, session) selection filter."""

        self.scan_type = opts.get('scan_type', 'dce')
        """The ``dce`` or ``t2`` scan type."""

    def __iter__(self):
        return self.next()

    def next(self):
        """
        Iterates over the visits in the subject directories as described in
        :meth:`iter_new_visits`.
        """
        # The visit subdirectory match pattern.
        vpat = self.collection.session_pattern
        
        # The DICOM file search pattern depends on the scan type.
        dcm_pat = self.collection.dcm_pat_dict[self.scan_type]
        
        # Iterate over the visits.
        with xnat_helper.connection():
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
                        # If the scan type is not dce, then qualify the
                        # session name with the scan type suffix.
                        if self.scan_type != 'dce':
                            sess += "_%s" % self.scan_type
                        
                        # Apply the selection filter, e.g. an XNAT existence
                        # check. If the session passes the filter, then
                        # the files qualify for iteration.
                        if self.filter(sbj, sess):
                            # The DICOM file match pattern.
                            file_pat = os.path.join(sess_dir, dcm_pat)
                            # The visit directory DICOM file iterator.
                            dcm_file_iter = glob.iglob(file_pat)
                            logger(__name__).debug("Discovered session %s in"
                                                   " %s" % (sess, sess_dir))
                            yield (sbj, sess, dcm_file_iter)
