import os
import re
import gzip
import operator
import dicom
from dicom.filereader import InvalidDicomError
from .file_helper import FileIterator
from .logging_helper import logger


def read_dicom_file(fp, *args):
    """
    Reads the given DICOM file. If the file extension ends in ``.gz``, then the
    content is uncompressed before reading.
    
    :param fp: the file pathname or stream
    :param args: the remaining pydicom read_file arguments
    :return: the pydicom dicom object
    :raise: InvalidDicomError if the file is not a DICOM file
    :raise: IOError if the file cannot be read
    """
    if isinstance(fp, str):
        logger(__name__).debug("Opening the DICOM file %s..." % fp)
        _, ext = os.path.splitext(fp)
        if ext == '.gz':
            fp = gzip.open(fp)
        else:
            fp = open(fp)
    return dicom.read_file(fp, *args)


def read_dicom_header(fp):
    """
    Reads the DICOM header of the given file.
    
    :param fp: the file pathname or stream
    :return: the pydicom dicom object without the non-pixel tags
    :raise: InvalidDicomError if the file is not a DICOM file
    :raise: IOError if the file cannot be read
    """
    return read_dicom_file(fp, *DicomHeaderIterator.OPTS)


def isdicom(fp):
    """
    :param fp: the file path or stream
    :return: whether the file is a DICOM file
    :raise: IOError if the file cannot be read
    """
    try:
        read_dicom_header(fp)
    except InvalidDicomError:
        logger(__name__).debug("%s is not a DICOM file." % fp)
        return False
    return True


def select_dicom_tags(ds, *tags):
    """
    Reads the given DICOM dataset tags.
    
    :param ds: the pydicom dicom object
    :param tags: the names of tags to read (default all unbracketed tags)
    :return: the tag name => value dictionary
    """
    if not tags:
        # Skip tags with a bracketed name.
        tags = [de.name for de in ds if de.name[0] != '[']
    tdict = {}
    for t in tags:
        try:
            # The tag attribute.
            tattr = re.sub('\W', '', t)
            # Collect the tag value.
            tdict[t] = operator.attrgetter(tattr)(ds)
        except AttributeError:
            pass
    return tdict


def iter_dicom(*dicom_files):
    """
    Iterates over the DICOM data sets for DICOM files at the given locations.
    
    :param dicom_files: the DICOM files or directories containing DICOM files
    """
    return DicomIterator(*dicom_files)


def iter_dicom_headers(*dicom_files):
    """
    Iterates over the DICOM headers for DICOM files at the given locations.
    
    :param dicom_files: the DICOM files or directories containing DICOM files
    """
    return DicomHeaderIterator(*dicom_files)


class DicomIterator(FileIterator):

    """
    DicomIterator is a generator class for reading the pydicom data sets from
    DICOM files.
    """

    def __init__(self, *dicom_files):
        """
        :param dicom_files: the DICOM files to include
        """
        super(DicomIterator, self).__init__(*dicom_files)
        self.args = []

    def next(self):
        """
        Iterates over each DICOM data set.
        
        :yield: the next pydicom dicom object
        """
        for f in super(DicomIterator, self).next():
            try:
                yield read_dicom_file(f, *self.args)
            except InvalidDicomError:
                logger(__name__).info("Skipping non-DICOM file %s" % f)


class DicomHeaderIterator(DicomIterator):

    """
    DicomHeaderIterator is a generator class for reading the pydicom non-pixel
    data sets from DICOM files.
    """
    # Read the DICOM file with defer_size=256, stop_before_pixels=True and
    # force=False.
    OPTS = [256, True, False]

    def __init__(self, *dicom_files):
        super(DicomHeaderIterator, self).__init__(*dicom_files)
        self.args = DicomHeaderIterator.OPTS
