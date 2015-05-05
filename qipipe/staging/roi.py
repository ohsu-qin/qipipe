"""ROI utility functions."""

import os
import re
import glob
import qiutil

class ROIError(Exception):
    pass


class LesionROI(object):
    """
    Aggregate with attributes :attr:`lesion`, :attr:`slice` and :attr:`path`.
    """
    def __init__(self, lesion, slice_number, path):
        """
        :param lesion: the :attr:`lesion` value
        :param slice_number: the :attr:`slice` value
        :param path: the :attr:`path` value
        """
        self.lesion = lesion
        """The lesion number."""
        
        self.slice = slice_number
        """The slice number."""
        
        self.path = path
        """The absolute BOLERO ROI .bqf file path."""
    
    def __repr__(self):
        return str(dict(lesion=self.lesion, slice=self.slice, path=self.path))


def iter_roi(glob, regex, base_dir):
    """
    Iterates over the the BOLERO ROI mask files in the given input directory.
    This method is a :class:LesionROI generator, e.g.::

        >> # Find .bqf files anywhere under /path/to/session/processing.
        >> next(iter_roi('processing/*', '.*/\.bqf', '/path/to/session'))
        {lesion: 1, slice: 12, path: '/path/to/session/processing/rois/roi.bqf'}

    :param glob_pat: the glob match pattern
    :;param regex: the file name match regular expression
    :param base_dir: the AIRC source visit directory to search
    :yield: the :class:`LesionROI` objects
    """
    finder = qiutil.file.Finder(glob, regex)
    for match in finder.match(base_dir):
        # If there is no lesion qualifier, then there is only one lesion.
        lesion_s = match.group('lesion')
        lesion = int(lesion_s) if lesion_s else 1
        # If there is no slice index, then complain.
        slice_index_s = match.group('slice_index')
        if not slice_index_s:
            raise ROIError("The BOLERO ROI slice could not be determined" +
                           " from the file path: %s" % path)
        slice_ndx = int(slice_index_s)
        # Prepend the base directory to the matching file path.
        path = os.path.join(base_dir, match.group(0))
        
        yield LesionROI(lesion, slice_ndx, os.path.abspath(path))
