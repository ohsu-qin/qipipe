import re
from .staging_error import StagingError

T1_PAT = '*concat*/*'
"""The T1 scan directory match pattern."""

VOLUME_TAG = 'AcquisitionNumber'
"""
The DICOM tag which identifies the volume.
The AIRC collections are unusual in that the DICOM images which comprise
a 3D volume have the same DICOM Series Number and Acquisition Number tag.
The series numbers are consecutive, non-sequential integers, e.g. 9, 11,
13, ..., whereas the acquisition numbers are consecutive, sequential
integers starting at 1. The Acquisition Number tag is selected as the
volume number identifier.
"""

def collection_with_name(name):
    """
    :param name: the OHSU QIN collection name
    :return: the corresponding AIRC collection
    :raise ValueError: if the given collection name is not recognized
    """
    if not hasattr(collection_with_name, 'extent'):
        setattr(collection_with_name, 'extent', _create_collections())
    if name not in collection_with_name.extent:
        raise ValueError(
            "The AIRC collection name is not recognized: %s" % name)

    return collection_with_name.extent[name]


def _create_collections():
    """Creates the pre-defined AIRC collections."""

    # The AIRC T2 scan DICOM files are in special subdirectories.
    breast_t2_dcm_pat = '*sorted/2_tirm_tra_bilat/*'
    sarcoma_t2_dcm_pat = '*T2*/*'

    # The Breast T1 scan .bqf ROI files are in the session subdirectory
    # processing/<R10 directory>/slice<slice index>/, where
    # <R10 directory> can be qualified by a lesion number.
    breast_roi_glob = 'processing/R10_0.[456]*/slice*/*.bqf'
    breast_roi_regex = re.compile("""
        processing/                 # The visit processing subdirectory
        R10_0.[456]                 # The R10 series subdirectory
        (_L(?P<lesion>\d+))?/       # The lesion modifier
        slice(?P<slice_index>\d+)/  # The slice subdirectory
        (?P<fname>.*\.bqf)          # The ROI file base name
    """, re.VERBOSE)
    breast_roi_pats = ROIPatterns(glob=breast_roi_glob, regex=breast_roi_regex)
    breast_t1_pats = ScanPatterns(dicom=T1_PAT, roi=breast_roi_pats)
    breast_t2_pats = ScanPatterns(dicom=breast_t2_dcm_pat)
    breast_scan_pats = {1: breast_t1_pats, 2: breast_t2_pats}

    # The Sarcoma T1 scan .bqf ROI files are in the session subdirectory
    # <processing>/<R10 directory>/slice<slice index>/, and do not
    # have a lesion qualifier.
    sarcoma_roi_glob = '*processing*/R10_0.[3456]*/slice*/*.bqf'
    sarcoma_roi_regex = re.compile("""
        .*processing.*/             # The visit processing subdirectory
        R10_0.[3456].*/             # The R10 series subdirectory
        slice(?P<slice_index>\d+)/  # The slice subdirectory
        (?P<fname>.*\.bqf)          # The ROI file base name
    """, re.VERBOSE)
    sarcoma_roi_pats = ROIPatterns(glob=sarcoma_roi_glob,
                                   regex=sarcoma_roi_regex)
    sarcoma_t1_pats = ScanPatterns(dicom=T1_PAT, roi=sarcoma_roi_pats)
    sarcoma_t2_pats = ScanPatterns(dicom=sarcoma_t2_dcm_pat)
    sarcoma_scan_pats = {1: sarcoma_t1_pats, 2: sarcoma_t2_pats}

    # The Breast images are in BreastChemo<subject>/Visit<session>/.
    breast_opts = dict(subject='BreastChemo(\d+)', session='Visit(\d+)',
                       scan=breast_scan_pats, volume=VOLUME_TAG)

    # The Sarcoma images are in Subj_<subject>/Visit_<session> with
    # visit pattern variations, e.g. 'Visit_3', 'Visit3' and 'S4V3'
    # all match session 3.
    sarcoma_opts = dict(subject='Subj_(\d+)', session='(?:Visit_?|S\d+V)(\d+)',
                        scan=sarcoma_scan_pats, volume=VOLUME_TAG)

    return dict(Breast=AIRCCollection('Breast', **breast_opts),
                Sarcoma=AIRCCollection('Sarcoma', **sarcoma_opts))


class ROIPatterns(object):
    """Aggregate with attributes :attr:`glob` and :attr:`regex`."""

    def __init__(self, glob, regex):
        self.glob = glob
        """The ROI file glob pattern."""
        
        self.regex = regex
        """The ROI file name match regular expression."""

    def __repr__(self):
        return str(dict(glob=self.glob, regex=self.regex))


class ScanPatterns(object):
    """Aggregate with attributes :attr:`dicom` and :attr:`roi`."""

    def __init__(self, dicom, roi=None):
        self.dicom = dicom
        """The DICOM file match *glob* and *regex* patterns."""
        
        self.roi = roi
        """The :class:`ROIPatterns` object."""

    def __repr__(self):
        return str(dict(dicom=self.dicom, roi=self.roi))


class AIRCCollection(object):
    """The OHSU AIRC collection characteristics."""

    def __init__(self, name, **opts):
        """
        :param name: `self.name`
        :param opts: the following required arguments:
        :option subject: the subject directory regular expression match pattern
        :option session: the session directory regular expression match pattern
        :option roi: the {scan number: ROI patterns} dictionary, where the
            ROI patterns is a Bunch with items ``glob`` and ``regex``
        :option dicom: the
          {scan number: image file directory regular expression match pattern}
          dictionary
        :option volume: the DICOM tag which identifies a scan volume
        """
        self.name = name
        """The collection name."""

        self.subject_pattern = opts['subject']
        """The subject directory match pattern."""

        self.session_pattern = opts['session']
        """The subject directory match pattern."""

        self.scan_patterns = opts['scan']
        """The {scan number: :class:`ScanPatterns`} dictionary."""

        self.volume_tag = opts['volume']
        """The DICOM tag which identifies a scan volume."""

    def path2subject_number(self, path):
        """
        :param path: the directory path
        :return: the subject number
        :raise StagingError: if the path does not match the collection subject
            pattern
        """
        match = re.search(self.subject_pattern, path)
        if not match:
            raise StagingError(
                "The directory path %s does not match the subject pattern %s" %
                (path, self.subject_pattern))

        return int(match.group(1))

    def path2session_number(self, path):
        """
        :param path: the directory path
        :return: the session number
        :raise StagingError: if the path does not match the collection session
            pattern
        """
        match = re.search(self.session_pattern, path)
        if not match:
            raise StagingError(
                "The directory path %s does not match the session pattern %s" %
                (path, self.session_pattern))
        return int(match.group(1))
