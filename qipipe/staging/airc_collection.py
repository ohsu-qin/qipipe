import re
from .staging_error import StagingError

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

def _create_collections():
    """Creates the pre-defined AIRC collections."""

    # The AIRC T1 scan DICOM files are in the concat subdirectory.
    # The AIRC T2 scan DICOM files are in special subdirectories.
    breast_dcm_pat_dict = {1: T1_PAT, 2: '*sorted/2_tirm_tra_bilat/*'}
    sarcoma_dcm_pat_dict = {1: T1_PAT, 2: '*T2*/*'}

    # The Breast images are in BreastChemo*subject*/Visit*session*.
    breast_opts = dict(subject='BreastChemo(\d+)', session='Visit(\d+)',
                       dicom=breast_dcm_pat_dict, volume=VOLUME_TAG)

    # The Sarcoma images are in Subj_*subject*/Visit_*session* with
    # visit pattern variations, e.g. 'Visit_3', 'Visit3' and 'S4V3'
    # all match session 3.
    sarcoma_opts = dict(subject='Subj_(\d+)', session='(?:Visit_?|S\d+V)(\d+)',
                        dicom=sarcoma_dcm_pat_dict, volume=VOLUME_TAG)

    return dict(Breast=AIRCCollection('Breast', **breast_opts),
                Sarcoma=AIRCCollection('Sarcoma', **sarcoma_opts))


class AIRCCollection(object):
    """The OHSU AIRC collection characteristics."""

    def __init__(self, name, **opts):
        """
        :param name: `self.name`
        :param opts: the following required arguments:
        :option subject: the subject directory regular expression match pattern
        :option session: the session directory regular expression match pattern
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

        self.scan_dicom_patterns = opts['dicom']
        """The {scan number: image file directory match pattern} dictionary."""

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
