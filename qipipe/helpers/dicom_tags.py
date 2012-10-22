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
    h = ImageHierarchy()
    for f in files:
        if os.path.isfile(f):
            _add_to_image_hierarchy(f, h)
        elif os.path.isdir(f):
            for root, dirs, files in os.walk(f):
                for f in files:
                    path = os.path.join(root, f)
                    _add_to_image_hierarchy(path, h)
        else:
            raise InvalidDicomError("Can't read file " + f)
    return h

def _add_to_image_hierarchy(path, hierarchy):
    try:
        ds = read_tags(path)
    except InvalidDicomError:
        # Skip this non-DICOM file.
        pass
    else:
        hierarchy.add(ds)
    
