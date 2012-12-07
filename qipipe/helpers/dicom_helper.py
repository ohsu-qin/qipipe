"""
DICOM utilites.
"""

import os
import logging
from .dicom_tag_reader import DicomTagReader
from .image_hierarchy import ImageHierarchy
import dicom
from dicom.filereader import InvalidDicomError
from .file_helper import FileIterator

def isdicom(path):
    """
    @param path: the file path
    @return: whether the file is a DICOM file
    @raise: IOError if the file cannot be read
    """
    try:
        read_dicom_header(path)
    except InvalidDicomError:
        return False
    return True

def read_dicom_header(path):
    """
    Reads the given DICOM file tags
    
    @param path: the file pathname
    @return: the pydicom dicom object
    @raise: InvalidDicomError if the file is not a DICOM file
    @raise: IOError if the file cannot be read
    """
    # Read the DICOM file with defer_size=256, stop_before_pixels=True and force=False.
    return dicom.read_file(path, 256, True, False)

def read_dicom_tags(tags, *files):
    """
    @param tags: the tags to read
    @param files: the files to read
    @return: an iterator over the tag value lists
    """
    return DicomTagFilterIterator(tags, *files)

def read_image_hierarchy(*files):
    """
    Returns the ImageHierarchy for the DICOM files in the given locations.

    @param files: the files or directories to walk for DICOM files
    @return: the image hierarchy
    @rtype: ImageHierarchy
    """
    # Build the hierarchy dictionary.
    h = ImageHierarchy()
    for ds in DicomHeaderIterator(*files):
        h.add(ds)
    return h

class DicomHeaderIterator(FileIterator):
    """
    DicomHeaderIterator is a utility class for reading the pydicom non-pixel data sets from DICOM files.
    """
    
    def __init__(self, *files):
        super(DicomHeaderIterator, self).__init__(*files)
    
    def next(self):
        for f in super(DicomHeaderIterator, self).next():
            try:
                yield read_dicom_header(f)
            except InvalidDicomError:
                logging.info("Skipping non-DICOM file %s" % f)


class DicomTagFilterIterator(DicomHeaderIterator):
    """
    DicomTagFilterIterator is a utility class for reading a list of tag values from DICOM files.
    """
    
    def __init__(self, tags, *files):
        """
        @param tags: the tags to read
        @ param files: the files to read
        """
        super(DicomTagFilterIterator, self).__init__(*files) 
        self._tag_reader = DicomTagReader(*tags)
    
    def next(self):
        for ds in super(DicomTagFilterIterator, self).next():
            yield self._tag_reader.read(ds)

