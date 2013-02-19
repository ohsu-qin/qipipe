import os, re
from qipipe.helpers.dicom_helper import edit_dicom_headers
from .staging_error import StagingError
from .staging_helper import extract_patient_number_from_path
from .sarcoma_config import sarcoma_location

import logging
logger = logging.getLogger(__name__)

COLLECTIONS = set(['Breast', 'Sarcoma'])
"""The supported collection names."""

def fix_dicom_headers(source, dest, collection):
    """
    Fix the source OHSU QIN AIRC DICOM headers as follows:
        - Replace the C{Patient ID} value with the patient number, e.g. C{Sarcoma01}.
        - Add the C{Body Part Examined} tag.
    
    The supported collection names are defined in the C{COLLECTIONS} set.
    
    @param source: the input patient directory
    @param dest: the location in which to write the modified patient directory
    @param collection: the collection name
    @return: the files which were created
    @raise StagingError: if the collection is not supported
    """    
    parent = os.path.normpath(os.path.dirname(source))
    if not collection in COLLECTIONS:
        raise StagingError('Unrecognized collection: ' + collection)
    logger.debug("Fixing the DICOM headers in %s..." % source)
    # Extract the patient number from the patient directory name.
    pt_nbr = extract_patient_number_from_path(source)
    pt_id = "%(collection)s%(pt)02d" % {'collection': collection, 'pt': pt_nbr}
    # The tag name => value dictionary.
    tnv = {'PatientID': pt_id}
    if collection == 'Breast':
        tnv['BodyPartExamined'] = 'BREAST'
    elif collection == 'Sarcoma':
        tnv['BodyPartExamined'] = sarcoma_location(pt_id)
    # Set the tags in every image file.
    files = edit_dicom_headers(source, dest, tnv)
    logger.debug("Fixed the DICOM headers in %s." % source)
    return files
