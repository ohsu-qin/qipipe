import os
import re

SUBJECT_FMT = '%s%03d'
"""
The XNAT subject name format with argument collection and subject number.
"""

SESSION_FMT = 'Session%02d'
"""The XNAT session name format with argument session number."""

CONF_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'conf')
)
"""The common configuration directory."""

VOLUME_DIR_PAT = re.compile("volume(?P<volume_number>\d{3})$")
"""
The volume directory name pattern. The directory name is
``volume``*number*, where *number* is the zero-padded, one-based
volume number matched as the ``volume_number`` group, as determined
by the :meth:`qipipe.pipeline.staging.volume_format` function.
"""

VOLUME_FILE_PAT = re.compile("volume(?P<volume_number>\d{3}).nii.gz$")
"""
The volume file name pattern. The image file name is the
:const:`VOLUME_DIR_PAT` pattern followed by the extension
``.nii.gz``.
"""

SCAN_TS_BASE = 'scan_ts'
"""The XNAT scan time series file base name without extension."""

SCAN_TS_FILE = "%s.nii.gz" % SCAN_TS_BASE
"""The XNAT scan time series file name with extension."""

MASK_RESOURCE = 'mask'
"""The XNAT mask resource name."""

MASK_FILE = 'mask.nii.gz'
"""The XNAT mask file name with extension."""
