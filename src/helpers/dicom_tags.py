"""
DICOM tag utilites.
"""

import os
from .image_hierarchy import ImageHierarchy
import dicom
from dicom.filereader import InvalidDicomError

def read_tags(path):
    """
    Reads the given DICOM file tags
    
    :param path: the file pathname
    :return: the pydicom dicom object
    :raise: InvalidDicomError if the file is not a DICOM file
    """
    # Read the DICOM file with defer_size=256, stop_before_pixels=True and force=False.
    return dicom.read_file(path, 256, True, False)
    
def read_image_hierarchy(*files):
    """
    Returns the ImageHierarchy for the DICOM files in the given locations.

    :param files: the files or directories to walk for DICOM files
    :return: the image hierarchy
    :rtype: ImageHierarchy
    """
    # the hierarchy dictionary
    hierarchy = ImageHierarchy()
    for f in files:
        if os.path.isfile(f):
            _add_to_image_hierarchy(f, hierarchy)
        elif os.path.isdir(f):
            for root, dirs, files in os.walk(f):
                for f in files:
                    _add_to_image_hierarchy(f, hierarchy)
        else:
            raise InvalidDicomError("Can't read file " + f)
    return hierarchy

def _add_to_image_hierarchy(path, hierarchy):
    try:
        ds = read_tags(path)
    except InvalidDicomError:
        # Skip this non-DICOM file.
        pass
    else:
        hierarchy.add(ds)
    
