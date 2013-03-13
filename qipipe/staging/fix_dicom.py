import os, re
from ..helpers.dicom_helper import edit_dicom_headers
from .staging_helpers import match_series_hierarchy
from .sarcoma_config import sarcoma_location
from .staging_error import StagingError

import logging
logger = logging.getLogger(__name__)

def fix_dicom_headers(series_dir, dest, collection):
    """
    Fix the input OHSU QIN AIRC DICOM headers as follows:
        - Replace the C{Patient ID} value with the subject number, e.g. C{Sarcoma01}.
        - Add the C{Body Part Examined} tag.

    The supported collection names are defined in the C{COLLECTIONS} set.
    
    @param series_dir: the input series directory
    @param dest: the location in which to write the modified subject directory
    @param collection: the collection name
    @return: the files which were created
    @raise StagingError: if the collection is not supported
    """    
    logger.debug("Fixing the DICOM headers in %s..." % series_dir)

    # Infer the subject id from the directory name.
    sbj_id, _, _ = match_series_hierarchy(series_dir)
    
    # The tag name => value dictionary.
    tnv = {'PatientID': sbj_id}
    if collection == 'Sarcoma':
        tnv['BodyPartExamined'] = sarcoma_location(sbj_id)
    else:
        tnv['BodyPartExamined'] = collection.upper()
    
    # Set the tags in every image file.    
    files = edit_dicom_headers(series_dir, dest, tnv)

    logger.debug("Fixed the DICOM headers in %s." % series_dir)
    
    return files
