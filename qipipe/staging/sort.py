import os
from shutil import copy
import qidicom.hierarchy
from ..helpers.logging import logger
from . import (image_collection, iterator)
from .staging_error import StagingError


def sort(collection, scan, *in_dirs):
    """
    Groups the DICOM files in the given location by volume.

    :param collection: the collection name
    :param scan: the scan number
    :param in_dirs: the input DICOM directories
    :return: the {volume: files} dictionary
    """
    # Get the collection pattern.
    coll = image_collection.with_name(collection)
    scan_patterns = coll.patterns.scan.get(scan)
    if not scan_patterns:
        raise StagingError("There is no pattern for collection %s" +
                           " scan %d" %(collection, scan))
    tag = coll.patterns.volume

    return _sort(tag, *in_dirs)


def _sort(tag, *in_dirs):
    """
    :param tag: the DICOM meta-data volume tag
    :param in_dirs: the input DICOM directories
    :return: the {volume: files} dictionary
    """
    _logger = logger(__name__)
    in_dirs_s = in_dirs[0] if len(in_dirs) == 1 else [d for d in in_dirs]
    _logger.debug("Sorting the DICOM files in %s..." % in_dirs_s)
    vol_dict = qidicom.hierarchy.group_by(tag, *in_dirs)
    file_cnt = sum((len(files) for files in vol_dict.itervalues()))
    _logger.debug("Sorted %d DICOM files into %d volumes." %
                  (file_cnt, len(vol_dict)))

    return vol_dict;
