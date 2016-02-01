"""
OHSU - ROI utility functions.

TODO - move this to ohsu-qipipe.
"""

import os
import re
import glob
import qiutil


class ROIError(Exception):
    pass


class LesionROI(object):
    """
    Aggregate with attributes :attr:`lesion`, :attr:`slice` and :attr:`location`.
    """
    def __init__(self, lesion, slice_sequence_number, location):
        """
        :param lesion: the :attr:`lesion` value
        :param slice_number: the :attr:`slice` value
        :param location: the :attr:`location` value
        """
        self.lesion = lesion
        """The lesion number."""
        
        self.slice = slice_sequence_number
        """The one-based slice sequence number."""
        
        self.location = location
        """The absolute BOLERO ROI .bqf file path."""
    
    def __repr__(self):
        return (self.__class__.__name__ +
                str(dict(lesion=self.lesion, slice=self.slice,
                         location=self.location)))


def iter_roi(glob, regex, input_dir):
    """
    Iterates over the the OHSU ROI ``.bqf`` mask files in the given
    input directory. This method is a :class:`LesionROI` generator,
    e.g.::
        
        >>> # Find .bqf files anywhere under /path/to/session/processing.
        >>> next(iter_roi('processing/*', '.*/\.bqf', '/path/to/session'))
        {lesion: 1, slice: 12, path: '/path/to/session/processing/rois/roi.bqf'}
    
    :param glob: the glob match pattern
    :;param regex: the file name match regular expression
    :param input_dir: the source session directory to search
    :yield: the :class:`LesionROI` objects
    """
    finder = qiutil.file.Finder(glob, regex)
    for match in finder.match(input_dir):
        # If there is no lesion qualifier, then there is only one lesion.
        try:
            lesion_s = match.group('lesion')
        except IndexError:
            lesion_s = None
        lesion = int(lesion_s) if lesion_s else 1
        # If there is no slice index, then complain.
        slice_seq_nbr_s = match.group('slice_sequence_number')
        if not slice_seq_nbr_s:
            raise ROIError("The ROI slice could not be determined" +
                           " from the file path: %s" % path)
        slice_seq_nbr = int(slice_seq_nbr_s)
        # Prepend the base directory to the matching file path.
        path = os.path.join(input_dir, match.group(0))
        
        yield LesionROI(lesion, slice_seq_nbr, os.path.abspath(path))
