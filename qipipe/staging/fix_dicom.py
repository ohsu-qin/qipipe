import os, re
from ..helpers.dicom_helper import edit_dicom_headers
from .sarcoma_config import sarcoma_location
from .staging_error import StagingError

import logging
logger = logging.getLogger(__name__)

def fix_dicom_headers(collection, subject, *dicom_files, **opts):
    """
    Fix the given input OHSU QIN AIRC DICOM files as follows:
        - Replace the C{Patient ID} value with the subject number, e.g. C{Sarcoma001}
        - Add the C{Body Part Examined} tag
        - Standardize the file name
        
    The output file name is standardized as follows:
        - The file name is lower-case
        - The file extension is C{.dcm}
        - Each non-word character is replaced by an underscore

    The supported collection names are defined in the C{COLLECTIONS} set.
    
    @param collection: the collection name
    @param subject: the input subject name
    @param opts: the keyword options
    @keyword dest: the location in which to write the modified files (default current directory)
    @return: the files which were created
    @raise StagingError: if the collection is not supported
    """    

    # Make the tag name => value dictionary.
    if collection == 'Sarcoma':
        site = sarcoma_location(subject)
    else:
        site = collection.upper()
    tnv = dict(PatientID=subject, BodyPartExamined=site)
    
    # Set the tags in each image file.
    if 'dest' in opts:
        dest = opts['dest']
    else:
        dest = os.getcwd()
    edited = edit_dicom_headers(dest, *dicom_files, **tnv)
    
    # Rename the edited files as necessary.
    out_files = []
    for f in edited:
        std_name = _standardize_dicom_file_name(f)
        if f != std_name:
            os.rename(f, std_name)
            out_files.append(std_name)
        logger.debug("The DICOM headers in %s were fixed and saved as %s." % (f, std_name))

    return out_files
    
def _standardize_dicom_file_name(path):
    """
    Standardizes the given input file name as follows:
    """
    fdir, fname = os.path.split(path)
    # Replace non-word characters.
    fname = re.sub('\W', '_', fname.lower())
    # Add a .dcm extension, if necessary.
    _, ext = os.path.splitext(fname)
    if not ext:
        fname = fname + '.dcm'
    return os.path.join(fdir, fname)
