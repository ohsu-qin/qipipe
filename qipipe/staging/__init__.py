"""Image processing preparation.

The staging package defines the functions used to prepare the study image files for import into XNAT,
submission to the TCIA QIN collections and pipeline processing.
"""
from .group_dicom import group_dicom_files
from .fix_dicom import fix_dicom_headers
from .ctp import create_ctp_id_map
