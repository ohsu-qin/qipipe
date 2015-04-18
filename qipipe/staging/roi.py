"""ROI utility functions."""

import os
import re
import glob
from . import airc_collection as airc

class ROIError(Exception):
    pass

def iter_roi(collection, base_dir):
    """
    Iterates over the the BOLERO ROI mask files in the given input directory.
    This method is a generator which yields a tuple consisting
    of the lesion number, slice number and .bqf files, e.g.::
    
        >> lesion, slice, filename = next(iter_roi('Sarcoma', '/path/to/session'))
        >> print lesion
        1
        >> print slice
        12
        >> print filename
        '/path/to/session/roi.bqf'

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source visit directories to search
    :yield: the (lesion number, slice index, file path) tuple:
    :yieldparam lesion: the lesion number
    :yieldparam slice: the slice number
    :yieldparam path: the absolute BOLERO ROI .bqf file path
    """
    # Validate that there is a collection.
    if not collection:
        raise ValueError('The ROI helper is missing the AIRC collection name')
    
    # Validate that there is a base directory.
    if not base_dir:
        raise ValueError('The ROI helper is missing the search base directory')

    airc_coll = airc.collection_with_name(collection)
    files = glob.iglob('/'.join((base_dir, airc_coll.roi_patterns.glob)))
    
    matcher = airc_coll.roi_patterns.regex
    for path in files:
        prefix_len = len(base_dir) + 1
        rel_path = path[prefix_len:]
        match = matcher.match(rel_path)
        if match:
            # If there is no lesion qualifier, then there is only one lesion.
            lesion_s = match.group('lesion')
            lesion = int(lesion_s) if lesion_s else 1
            # If there is on slice index, then complain.
            slice_index_s = match.group('slice_index')
            if not slice_index_s:
                raise ROIError("The BOLERO ROI slice could not be determined" +
                               " from the file path: %s" % path)
            slice_ndx = int(slice_index_s)
            yield lesion, slice_ndx, os.path.abspath(path)
