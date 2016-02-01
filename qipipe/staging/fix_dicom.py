import os
import re
from datetime import datetime
import functools
import dicom
from qiutil.logging import logger
from qiutil import dates
from qidicom import (meta, writer)
from .sarcoma_config import sarcoma_location

DATE_FMT = '%Y%m%d'
"""The DICOM date format is YYYYMMDD."""


COMMENT_PREFIX = re.compile('^TTC \d+(\/.\d*)? sec')
"""*OHSU* - the ``Image Comments`` tag value prefix."""


def fix_dicom_headers(collection, subject, *in_files, **opts):
    """
    Fix the given input DICOM files as follows:
    
    * Replace the ``Patient ID`` value with the subject number, e.g.
        ``Sarcoma001``
    
    * Add the ``Body Part Examined`` tag
    
    * Anonymize the ``Patient's Birth Date`` tag
    
    * Standardize the file name
    
    *OHSU* - The ``Body Part Examined`` tag is set as follows:
    
    * If the collection is ``Sarcoma``, then the body part is the
        :meth:`qipipe.staging.sarcoma_config.sarcoma_location`.
    
    * Otherwise, the body part is the capitalized collection name, e.g.
        ``BREAST``.
    
    *OHSU* - Remove extraneous ``Image Comments`` tag value content
    which might contain PHI.
    
    The output file name is standardized as follows:
    
    * The file name is lower-case
    
    * The file extension is ``.dcm``
    
    * Each non-word character is replaced by an underscore
    
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
    # The tag editor.
    editor = meta.Editor(PatientID=subject, BodyPartExamined=site,
                         PatientsBirthDate=_anonymize_date,
                         ImageComments=_scrub_comment)
    # The destination directory.
    dest = opts.get('dest', os.getcwd())
    # The destination file namer.
    file_namer = functools.partial(_dest_file_name, dest=dest)
    logger(__name__).debug("Replacing the following DICOM tags: %s" %
                           editor.edits.keys())
    # An array to collect the edited files.
    dcm_files = []
    # Edit the DICOM files.
    for ds in writer.edit(*in_files, dest=file_namer):
        editor.edit(ds)
        dcm_files.append(ds.filename)
    logger(__name__).debug("Changed the DICOM tag values.")
    
    # Return the output file names.
    return [file_namer(f) for f in dcm_files]


def _anonymize_date(date):
    """
    :param date: the input date string
    :return: the anonymized date
    :rtype: str
    """
    # The unanonymized PHI date.
    phi = datetime.strptime(date, DATE_FMT)
    # The anonymized date.
    anon = dates.anonymize(phi)
    
    # Return the anonymized date as a string.
    return anon.strftime(DATE_FMT)


def _scrub_comment(comment):
    """
    :param comment: the comment tag value
    :return: the scrubbed comment
    """
    match = COMMENT_PREFIX.match(comment)
    
    return match.group(0) if match else comment


def _dest_file_name(in_file, dest):
    """
    Standardizes the given input file name.
    
    :param in_file: the input file name
    :param dest: the destination directory
    :return: the target output file name
    """
    _, fname = os.path.split(in_file)
    # Replace non-word characters.
    fname = re.sub('\W', '_', fname.lower())
    # Add a .dcm extension, if necessary.
    _, ext = os.path.splitext(fname)
    if not ext:
        fname = fname + '.dcm'
    
    return os.path.join(dest, fname)
