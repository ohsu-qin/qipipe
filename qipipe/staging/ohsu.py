"""
This module contains the OHSU-specific image collections.

The following OHSU QIN scan numbers are captured:
    * 1: T1
    * 2: T2
    * 4: DW
    * 6: PD
These scans have DICOM files specified by the
:attr:`qipipe.staging.image_collection.Collection.patterns`
``dicom`` attribute. The T1 scan has ROI files as well, specified
by the patterns ``roi.glob`` and ``roi.regex`` attributes.
"""

import re
from bunch import (Bunch, bunchify)
from .image_collection import Collection
from .staging_error import StagingError

MULTI_VOLUME_SCAN_NUMBERS = [1]
"""Only T1 scans can have more than one volume."""

# TODO - Move all of this to a new ohsu-qipipe.collections modules,
# which is responsible for creating the collections.
# Get as much as possible from a new ohsu-qipipe staging.cfg, e.g.:
# [Breast]
# numbers = {1: 't2', 2: 't1', 3: 'dw', 4: 'pd'}
# subject = BreastChemo(\d+)
# ...
# [Sarcoma]
# ...

BREAST_SUBJECT_REGEX = re.compile('BreastChemo(\d+)')
"""The Breast subject directory match pattern."""

SARCOMA_SUBJECT_REGEX = re.compile('Subj_(\d+)')
"""The Sarcoma subject directory match pattern."""

SESSION_REGEX_PAT = """
    (?:             # Don't capture the prefix
     [vV]isit       # The Visit or visit prefix form
     _?             # with an optional underscore delimiter
     |              # ...or...
     %s\d+_?V       # The alternate prefix form, beginning with
                    # a leading collection abbreviation
                    # substituted into the pattern below
    )               # End of the prefix
    (\d+)$          # The visit number
"""
"""
The session directory match pattern. This pattern must be specialized
for each collection by replacing the %s place-holder with a string.
"""

BREAST_SESSION_REGEX = re.compile(SESSION_REGEX_PAT % 'BC?', re.VERBOSE)
"""
The Sarcoma session directory match pattern. The variations
``Visit_3``, ``Visit3``, ``visit3``, ``BC4V3``, ``BC4_V3`` and ``B4V3``
all match Breast Session03.
"""

SARCOMA_SESSION_REGEX = re.compile(SESSION_REGEX_PAT % 'S', re.VERBOSE)
"""
The Sarcoma session directory match pattern. The variations
``Visit_3``, ``Visit3``, ``visit3`` ``S4V3``, and ``S4_V3`` all match
Sarcoma Session03.
"""

T1_PAT = '*concat*'
"""The T1 DICOM directory match pattern."""

BREAST_T2_PAT = '*sorted/2_tirm_tra_bilat'
"""The Breast T2 DICOM directory match pattern."""

SARCOMA_T2_PAT = '*T2*'
"""The Sarcoma T2 DICOM directory match pattern."""

BREAST_DW_PAT = '*sorted/*Diffusion'
"""The Breast DW DICOM directory match pattern."""

SARCOMA_DW_PAT = '*Diffusion'
"""The Sarcoma DW DICOM directory match pattern."""

BREAST_PD_PAT = '*sorted/*PD*'
"""The Breast pseudo-proton density DICOM directory match pattern."""

BREAST_ROI_PAT = 'processing/R10_0.[456]*/slice*'
"""
The Breast ROI glob filter. The ``.bqf`` ROI files are in the
following session subdirectory:

    processing/<R10 directory>/slice<slice index>/
"""

BREAST_ROI_REGEX = re.compile("""
    ^.*                         # The optional parent scan directory
    processing/                 # The visit processing subdirectory
    R10_0\.[456]                # The R10 series subdirectory
     (_L                        # The optional lesion modifier
      (?P<lesion>\d+)           # The lesion number
     )?                         # End of the lesion modifier
     /                          # End of the R10 subdirectory
    slice                       # The slice subdirectory
     (?P<slice_sequence_number>\d+)       # The slice index
     /                          # End of the slice subdirectory
    (?P<fname>                  # The ROI file base name
     .*\.bqf                    # The ROI file extension
    )$                          # End of the ROI file name
""", re.VERBOSE)
"""
The Breast ROI .bqf ROI file match pattern.
"""

SARCOMA_ROI_PAT = 'Breast processing results/multi_slice/slice*'
"""
The Sarcoma ROI glob filter. The ``.bqf`` ROI files are in the
session subdirectory:

    results/<ROI directory>/slice<slice index>/
"""

SARCOMA_ROI_REGEX = re.compile("""
    ^.*                         # The optional parent scan directory
    results/                    # The visit processing subdirectory
    ROI_ave(rage)?/             # The ROI subdirectory
    taui_.*d001/                # An intermediate sudirectory
    slice(?P<slice_sequence_number>\d+)/  # The slice subdirectory
    (?P<fname>                  # The ROI file base name
     .*\.bqf                    # The ROI file extension
    )$                          # End of the ROI file name
""", re.VERBOSE)
"""
The Sarcoma ROI .bqf ROI file match pattern.

:Note: The Sarcoma ROI directories are inconsistently named, with several
    alternatives and duplicates.

    TODO - clarify which of the Sarcoma ROI naming variations should be used.

:Note: There are no apparent lesion number indicators in the Sarcoma ROI
    input.

    TODO - confirm that there is no Sarcoma lesion indicator.
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


def _create_breast_collection():
    """:return: the OHSU AIRC Breast collection"""
    roi = Bunch(glob=BREAST_ROI_PAT, regex=BREAST_ROI_REGEX)
    t1 = Bunch(dicom=T1_PAT, roi=roi)
    t2 = Bunch(dicom=BREAST_T2_PAT)
    dwi = Bunch(dicom=BREAST_DW_PAT)
    pd = Bunch(dicom=BREAST_PD_PAT)
    scan_types = {1: 'T1', 2: 'T2', 4: 'DW', 6: 'PD'}
    scan = {1: t1, 2: t2, 4: dwi, 6: pd}
    opts = dict(crop_posterior=True, scan_types=scan_types,
                subject=BREAST_SUBJECT_REGEX, session=BREAST_SESSION_REGEX,
                scan=scan, volume=VOLUME_TAG)
    return Collection('Breast', **opts)


def _create_sarcoma_collection():
    """:return: the OHSU AIRC Sarcoma collection"""
    roi = Bunch(glob=SARCOMA_ROI_PAT, regex=SARCOMA_ROI_REGEX)
    t1 = Bunch(dicom=T1_PAT, roi=roi)
    t2 = Bunch(dicom=SARCOMA_T2_PAT)
    dw = Bunch(dicom=SARCOMA_DW_PAT)
    scan_types = {1: 'T1', 2: 'T2', 4: 'DW'}
    scan = {1: t1, 2: t2, 4: dw}
    opts = dict(scan_types=scan_types, subject=SARCOMA_SUBJECT_REGEX,
                session=SARCOMA_SESSION_REGEX, scan=scan, volume=VOLUME_TAG)
    return Collection('Sarcoma', **opts)


def _create_collections():
    """:return: the OHSU AIRC collection"""
    return dict(breast=_create_breast_collection(),
                sarcoma=_create_sarcoma_collection())

collections = bunchify(_create_collections())
