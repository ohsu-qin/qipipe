import os
import logging
import re
from qipipe.helpers.dicom_helper import edit_dicom_headers
from .staging_error import StagingError
from .staging_helper import extract_trailing_integer_from_path
from .sarcoma_config import sarcoma_location

def fix_dicom_headers(dest, *dirs):
    """
    Fix the source OHSU QIN AIRC DICOM headers as follows:
        - Replace the C{Patient ID} value with the patient number, e.g. C{Sarcoma01}.
        - Add the C{Body Part Examined} tag.
    
    The parent directory of each input patient directory must be named either C{breast} or C{sarcoma}.
    @param dest: the directory in which to write the modified DICOM files
    @param dirs: the input patient directories
    """
    
    for d in dirs:
        logger.debug("Fixing the DICOM headers in %s..." % d)
        parent = os.path.normpath(os.path.dirname(d))
        histology = os.path.basename(parent).lower().capitalize()
        if not histology in ['Breast', 'Sarcoma']:
            raise StagingError('The parent directory name is not breast or sarcoma: ' + d)
        # Extract the patient number from the patient directory name.
        pt_nbr = extract_trailing_integer_from_path(d)
        pt_id = "%(histology)s%(pt)02d" % {'hist': histology, 'pt': pt_nbr}
        # The tag name => value dictionary.
        tnv = {'PatientID': pt_id}
        if histology == 'Breast':
            tnv['BodyPartExamined'] = 'BREAST'
        elif histology == 'Sarcoma':
            tnv['BodyPartExamined'] = sarcoma_location(pt_id)
        # Set the tags in every image file.
        edit_dicom_headers(d, dest, tnv)
        logger.debug("Fixed the DICOM headers in %s." % d)
