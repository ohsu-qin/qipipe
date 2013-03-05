import os, gzip, operator
import dicom
from dicom.filereader import InvalidDicomError
from .file_helper import FileIterator

import logging
logger = logging.getLogger(__name__)

def read_dicom_file(fp, *args):
    """
    Reads the given DICOM file. If the file extension ends in C{.gz}, then the
    content is uncompressed before reading.
    
    @param fp: the file pathname or stream
    @param args: the remaining pydicom read_file arguments
    @return: the pydicom dicom object
    :raise: InvalidDicomError if the file is not a DICOM file
    :raise: IOError if the file cannot be read
    """
    if isinstance(fp, str):
        logger.debug("Opening the file %s..." % fp)
        _, ext = os.path.splitext(fp)
        if ext == '.gz':
            fp = gzip.open(fp)
        else:
            fp = open(fp)
    return dicom.read_file(fp, *args)

def read_dicom_header(fp):
    """
    Reads the DICOM header of the given file.
    
    @param fp: the file pathname or stream
    @return: the pydicom dicom object without the non-pixel tags
    :raise: InvalidDicomError if the file is not a DICOM file
    :raise: IOError if the file cannot be read
    """
    return read_dicom_file(fp, *DicomHeaderIterator.OPTS)

def isdicom(fp):
    """
    @param fp: the file path or stream
    @return: whether the file is a DICOM file
    :raise: IOError if the file cannot be read
    """
    try:
        read_dicom_header(fp)
    except InvalidDicomError:
        logger.debug("%s is not a DICOM file." % fp)
        return False
    return True

def select_dicom_tags(ds, *tags):
    """
    Reads the given DICOM dataset tags.
    
    @param ds: the pydicom dicom object
    @param tags: the names of tags to read (default all unbracketed tags)
    @return: the tag name => value dictionary
    """
    if not tags:
        # Skip tags with a bracketed name.
        tags = [de.name for de in ds if de.name[0] != '[']
    tdict = {}
    for t in tags:
        try:
            # The tag attribute.
            tattr = t.replace(' ', '')
            # Collect the tag value.
            tdict[t] = operator.attrgetter(tattr)(ds)
        except AttributeError:
            pass
    return tdict

def iter_dicom(*paths):
    """
    Iterates over the DICOM data sets for DICOM files at the given locations.
    
    @param paths: the DICOM files or directories containing DICOM files
    """
    return DicomIterator(*paths)

def iter_dicom_headers(*paths):
    """
    Iterates over the DICOM headers for DICOM files at the given locations.
    
    @param paths: the DICOM files or directories containing DICOM files
    """
    return DicomHeaderIterator(*paths)

class DicomIterator(FileIterator):
    """
    DicomIterator is a utility class for reading the pydicom data sets from DICOM files.
    """
    def __init__(self, *paths):
        super(DicomIterator, self).__init__(*paths)
        self.args = []
    
    def next(self):
        """
        Iterates over each DICOM data set.
        """
        for f in super(DicomIterator, self).next():
            try:
                yield read_dicom_file(f, *self.args)
            except InvalidDicomError:
                logger.info("Skipping non-DICOM file %s" % f)

class DicomHeaderIterator(DicomIterator):
    """
    DicomHeaderIterator is a utility class for reading the pydicom non-pixel data sets from DICOM files.
    """
    # Read the DICOM file with defer_size=256, stop_before_pixels=True and force=False.
    OPTS = [256, True, False]
    
    def __init__(self, *paths):
        super(DicomHeaderIterator, self).__init__(*paths)
        self.args = DicomHeaderIterator.OPTS
            