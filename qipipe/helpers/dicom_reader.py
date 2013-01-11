import logging
import operator
import dicom
from dicom.filereader import InvalidDicomError
from .file_helper import FileIterator

def read_dicom_header(path):
    """
    Reads the DICOM header of the given file.
    
    @param path: the file pathname
    @return: the pydicom dicom object
    :raise: InvalidDicomError if the file is not a DICOM file
    :raise: IOError if the file cannot be read
    """
    
    # Read the DICOM file with defer_size=256, stop_before_pixels=True and force=False.
    logging.debug("Reading DICOM header in file %s..." % path)
    return dicom.read_file(path, 256, True, False)

def isdicom(path):
    """
    @param path: the file path
    @return: whether the file is a DICOM file
    :raise: IOError if the file cannot be read
    """
    
    try:
        read_dicom_header(path)
    except InvalidDicomError:
        logging.debug("%s is not a DICOM file." % path)
        return False
    return True

def select_dicom_tags(ds, *tags):
    """
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
            tdict[t] = operator.attrgetter(t.replace(' ', ''))(ds)
        except AttributeError:
            pass
    return tdict

def iter_dicom_headers(*paths):
    """
    Iterates over the DICOM headers for DICOM files at the given locations.
    
    @param paths: the DICOM files or directories containing DICOM files
    """
    
    return DicomHeaderIterator(*paths)

class DicomHeaderIterator(FileIterator):
    """
    DicomHeaderIterator is a utility class for reading the pydicom non-pixel data sets from DICOM files.
    """
    
    def __init__(self, *paths):
        super(DicomHeaderIterator, self).__init__(*paths)
    
    def next(self):
        """
        Iterates over each DICOM header.
        """
        for f in super(DicomHeaderIterator, self).next():
            try:
                yield read_dicom_header(f)
            except InvalidDicomError:
                logging.info("Skipping non-DICOM file %s" % f)
            