import os
from shutil import copy
import qidicom.hierarchy
from ..helpers.logging import logger
from . import (image_collection, iterator)
from .staging_error import StagingError


def sort(collection, scan, in_dir):
    """
    Groups the DICOM files in the given location by volume.

    :param collection: the collection name
    :param scan: the scan number
    :param in_dir: the input DICOM directory
    :return: the {volume: files} dictionary
    """
    # Get the collection pattern.
    coll = image_collection.with_name(collection)
    scan_patterns = coll.patterns.scan.get(scan)
    if not scan_patterns:
        raise StagingError("There is no pattern for collection %s" +
                           " scan %d" %(collection, scan))
    tag = coll.patterns.volume

    return _sort(in_dir, tag);


def _sort(in_dir, tag):
    """
    :param in_dir: the input DICOM directory
    :param tag: the DICOM meta-data volume tag
    :return: the {volume: files} dictionary
    """
    _logger = logger(__name__)
    _logger.debug("Sorting the DICOM files in %s..." % in_dir)
    vol_dict = qidicom.hierarchy.group_by(tag, in_dir)
    file_cnt = sum((len(files) for files in vol_dict.itervalues()))
    _logger.debug("Sorted %d DICOM files into %d volumes." %
                  (file_cnt, len(vol_dict)))

    return vol_dict;
