import os, re
from ..helpers.dicom_helper import edit_dicom_headers
from . import airc_study
from .staging_error import StagingError
from .sarcoma_config import sarcoma_location

import logging
logger = logging.getLogger(__name__)

def fix_dicom_headers(source, dest, collection):
    """
    Fix the source OHSU QIN AIRC DICOM headers as follows:
        - Replace the C{Patient ID} value with the subject number, e.g. C{Sarcoma01}.
        - Add the C{Body Part Examined} tag.

    The supported collection names are defined in the C{COLLECTIONS} set.
    
    @param source: the input subject directory
    @param dest: the location in which to write the modified subject directory
    @param collection: the collection name
    @return: the files which were created
    @raise StagingError: if the collection is not supported
    """    
    parent = os.path.normpath(os.path.dirname(source))
    try:
        study = airc_study.for_collection(collection)
    except KeyError:
        raise StagingError('Unrecognized collection: ' + collection)
    logger.debug("Fixing the DICOM headers in %s..." % source)
    
    # Extract the subject number from the subject directory name.
    pt_nbr = study.path2subject_number(source)
    pt_id = "%(collection)s%(pt_nbr)02d" % {'collection': collection, 'pt_nbr': pt_nbr}
    
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
