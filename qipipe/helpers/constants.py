import os

SUBJECT_FMT = '%s%03d'
"""
The XNAT subject name format with argument collection and subject number.
"""

SESSION_FMT = 'Session%02d'
"""The XNAT session name format with argument session number."""

CONF_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf'))
"""The common configuration directory."""

SCAN_TS_RSC = 'scan_ts'
"""The XNAT scan time series resource name."""

MASK_RSC = 'mask'
"""The XNAT mask resource name."""
