import os
import re
from qidicom import meta
from .sarcoma_config import sarcoma_location

from qiutil.logging import logger


def fix_dicom_headers(collection, subject, *dicom_files, **opts):
    """
    Fix the given input OHSU QIN AIRC DICOM files as follows:

    - Replace the ``Patient ID`` value with the subject number, e.g.
        ``Sarcoma001``

    - Add the ``Body Part Examined`` tag

    - Standardize the file name

    The ``Body Part Examined`` tag is set as follows:

    - If the collection is ``Sarcoma``, then the body part is the
        :meth:`qipipe.staging.sarcoma_config.sarcoma_location`.
    
    - Otherwise, the body part is the capitalized collection name, e.g.
        ``BREAST``.
        
    The output file name is standardized as follows:

    - The file name is lower-case

    - The file extension is ``.dcm``

    - Each non-word character is replaced by an underscore
    
    :param collection: the collection name
    :param subject: the input subject name
    :param opts: the following keyword arguments:
    :keyword dest: the location in which to write the modified files
        (default is the current directory)
    :return: the files which were created
    :raise StagingError: if the collection is not supported
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
    edited = meta.edit(dest, *dicom_files, **tnv)

    # Rename the edited files as necessary.
    out_files = []
    for f in edited:
        std_name = _standardize_dicom_file_name(f)
        if f != std_name:
            os.rename(f, std_name)
            out_files.append(std_name)
        logger(__name__).debug(
            "The DICOM headers in %s were fixed and saved as %s." % (f, std_name))

    return out_files


def _standardize_dicom_file_name(path):
    """
    Standardizes the given input file name.
    """
    fdir, fname = os.path.split(path)
    # Replace non-word characters.
    fname = re.sub('\W', '_', fname.lower())
    # Add a .dcm extension, if necessary.
    _, ext = os.path.splitext(fname)
    if not ext:
        fname = fname + '.dcm'
    return os.path.join(fdir, fname)
