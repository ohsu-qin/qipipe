"""
This module contains the OHSU-specific image collections.

The following OHSU QIN scan numbers are captured:
    * 1: T1
    * 2: T2
    * 4: DWI
These scans have DICOM files specified by the
:attr:`qipipe.staging.collection.Collection.patterns`
:attr:`qipipe.staging.collection.Patterns.scan`
:attr:`qipipe.staging.collection.ScanPatterns.dicom`
attribute. The T1 scan has ROI files as well, specified by the
:attr:`qipipe.staging.collection.ScanPatterns.roi`
:attr:`qipipe.staging.collection.ROIPatterns.glob` and
:attr:`qipipe.staging.collection.ROIPatterns.regex` attributes
"""

import re
from .collection import (Collection, ScanPatterns, ROIPatterns)
from . import collections
from .staging_error import StagingError

BREAST_SUBJECT_REGEX = re.compile('BreastChemo(\d+)')
"""The Breast subject directory match pattern."""

SARCOMA_SUBJECT_REGEX = re.compile('Subj_(\d+)')
"""The Sarcoma subject directory match pattern."""

SESSION_REGEX_PAT = """
    (?:             # Don't capture the prefix
     Visit          # The simple prefix form
     _?             # An optional underscore delimiter
     |              # ...or...
     %s\d+V         # The alternate prefix form, beginning with the
                    # leading collection letter substituted below
    )               # End of the prefix
    (\d+)$          # The visit number
"""

BREAST_SESSION_REGEX = re.compile(SESSION_REGEX_PAT % 'BC?', re.VERBOSE)
"""
The Sarcoma session directory match pattern. The variations
``Visit_3``, ``Visit3``, ``BC4V3`` and ``B4V3`` all match Breast
session 3.
"""

SARCOMA_SESSION_REGEX = re.compile(SESSION_REGEX_PAT % 'S', re.VERBOSE)
"""
The Sarcoma session directory match pattern. The variations
``Visit_3``, ``Visit3`` and ``S4V3`` all match Sarcoma session 3.
"""

T1_PAT = '*concat*/*'
"""The T1 DICOM file match pattern."""

BREAST_T2_PAT = '*sorted/2_tirm_tra_bilat/*'
"""The Breast T2 DICOM file match pattern."""

SARCOMA_T2_PAT = '*T2*/*'
"""The Sarcoma T2 DICOM file match pattern."""

BREAST_DWI_PAT = '*sorted/*Diffusion/*'
"""The Breast DWI DICOM file match pattern."""

SARCOMA_DWI_PAT = '*Diffusion/*'
"""The Sarcoma DWI DICOM file match pattern."""

BREAST_ROI_PAT = 'processing/R10_0.[456]*/slice*/*.bqf'
"""
The Breast ROI glob filter. The ``.bqf`` ROI files are in the
following session subdirectory:

    processing/<R10 directory>/slice<slice index>/
"""

BREAST_ROI_REGEX = re.compile("""
    processing/                 # The visit processing subdirectory
    R10_0\.[456]                # The R10 series subdirectory
     (_L                        # The optional lesion modifier
      (?P<lesion>\d+)           # The lesion number
     )?                         # End of the lesion modifier
     /                          # End of the R10 subdirectory 
    slice                       # The slice subdirectory
     (?P<slice_index>\d+)       # The slice index
     /                          # End of the slice subdirectory
    (?P<fname>                  # The ROI file base name
     .*\.bqf                    # The ROI file extension
    )                           # End of the ROI file name
""", re.VERBOSE)
"""
The Breast ROI .bqf ROI file match pattern.
"""

SARCOMA_ROI_PAT = '*processing*/R10_0.[3456]*/slice*/*.bqf'
"""
The Sarcoma ROI glob filter. The ``.bqf`` ROI files are in the
session subdirectory:

    processing/<R10 directory>/slice<slice index>/

where <R10 directory> can be qualified by a lesion number.
"""

SARCOMA_ROI_REGEX = re.compile("""
    .*processing.*/             # The visit processing subdirectory
    R10_0\.[3456].*/            # The R10 series subdirectory
    slice(?P<slice_index>\d+)/  # The slice subdirectory
    (?P<fname>.*\.bqf)          # The ROI file base name
""", re.VERBOSE)
"""
The Sarcoma ROI .bqf ROI file match pattern.

TODO - clarify which of the myriad Sarcoma ROI naming variations should
be used. Until then, the SarcomaCollection ROI is disabled below.
"""

VOLUME_TAG = 'AcquisitionNumber'
"""
The DICOM tag which identifies the volume.
The OHSU QIN collections are unusual in that the DICOM images which
comprise a 3D volume have the same DICOM Series Number and Acquisition
Number tag. The series numbers are consecutive, non-sequential integers,
e.g. 9, 11, 13, ..., whereas the acquisition numbers are consecutive,
sequential integers starting at 1. The Acquisition Number tag is
selected as the volume number identifier.
"""

class BreastCollection(Collection):
    """The OHSU AIRC Breast collection."""

    def __init__(self):
        roi = ROIPatterns(glob=BREAST_ROI_PAT, regex=BREAST_ROI_REGEX)
        t1 = ScanPatterns(dicom=T1_PAT, roi=roi)
        t2 = ScanPatterns(dicom=BREAST_T2_PAT)
        dwi = ScanPatterns(dicom=BREAST_DWI_PAT)
        scan = {1: t1, 2: t2, 4: dwi}
        opts = dict(subject=BREAST_SUBJECT_REGEX,
                    session=BREAST_SESSION_REGEX,
                    scan=scan, volume=VOLUME_TAG)
        super(BreastCollection, self).__init__('Breast', **opts)


class SarcomaCollection(Collection):
    """The OHSU AIRC Sarcoma collection."""

    def __init__(self):
        roi = ROIPatterns(glob=SARCOMA_ROI_PAT, regex=SARCOMA_ROI_REGEX)
        # TODO - add the Sarcoma ROI pattern after the clarification cited
        # in the SARCOMA_ROI_REGEX TODO item.
        #t1 = ScanPatterns(dicom=T1_PAT, roi=roi)
        t1 = ScanPatterns(dicom=T1_PAT)
        t2 = ScanPatterns(dicom=SARCOMA_T2_PAT)
        dwi = ScanPatterns(dicom=SARCOMA_DWI_PAT)
        scan = {1: t1, 2: t2, 4: dwi}
        opts = dict(subject=SARCOMA_SUBJECT_REGEX,
                    session=SARCOMA_SESSION_REGEX,
                    scan=scan, volume=VOLUME_TAG)
        super(SarcomaCollection, self).__init__('Sarcoma', **opts)


# Create the OHSU QIN collections.
collections.add(BreastCollection(), SarcomaCollection())

