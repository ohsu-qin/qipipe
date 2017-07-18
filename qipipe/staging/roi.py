"""
OHSU - ROI utility functions.

TODO - move this to ohsu-qipipe.
"""

import os
import re
import glob
import qiutil
from ..helpers.logging import logger

PARAM_REGEX = re.compile('(?P<key>\w+)\s*\:\s*(?P<value>\w+)')
"""
The regex to parse a parameter file.
"""


class LesionROI(object):
    """
    Aggregate with attributes :attr:`lesion` :attr:`volume`,
    :attr:`slice` and :attr:`location`.
    """
    def __init__(self, lesion, volume_number, slice_sequence_number,
                 location):
        """
        :param lesion: the :attr:`lesion` value
        :param volume_number: the :attr:`volume` value
        :param slice_sequence_number: the :attr:`slice` value
        :param location: the :attr:`location` value
        """
        self.lesion = lesion
        """The lesion number."""

        self.volume = volume_number
        """The one-based volume number."""

        self.slice = slice_sequence_number
        """The one-based slice sequence number."""

        self.location = location
        """The absolute BOLERO ROI .bqf file path."""

    def __repr__(self):
        return (self.__class__.__name__ +
                str(dict(lesion=self.lesion, volume=self.volume,
                         slice=self.slice, location=self.location)))


def iter_roi(regex, *in_dirs):
    """
    Iterates over the the OHSU ROI ``.bqf`` mask files in the given
    input directories. This method is a :class:`LesionROI` generator,
    e.g.::

        >>> # Find .bqf files anywhere under /path/to/session/processing.
        >>> next(iter_roi('.*/\.bqf', '/path/to/session'))
        {lesion: 1, slice: 12, path: '/path/to/session/processing/rois/roi.bqf'}

    :;param regex: the file name match regular expression
    :param in_dirs: the ROI directories to search
    :yield: the :class:`LesionROI` objects
    """
    _logger = logger(__name__)
    for in_dir in in_dirs:
        # Find the .bqf ROI mask file.
        bqfs = glob.glob("%s/*.bqf" % in_dir)
        if not bqfs:
            _logger.debug("The ROI directory %s does not contain a"
                        " .bqf ROI file" % in_dir)
            continue
        if len(bqfs) > 1:
            _logger.debug("The ROI directory %s contains more than"
                          " one .bqf ROI file" % in_dir)
            continue
        bqf = bqfs[0]

        match = regex.match(bqf)
        if not match:
            _logger.debug("The ROI file %s does not match the following"
                          " pattern:\n%s" % (bqf, regex.pattern))
            continue
        # If there is no lesion qualifier, then there is only one lesion.
        try:
            lesion_s = match.group('lesion')
            lesion = int(lesion_s) if lesion_s else 1
        except IndexError:
            lesion = 1

        # Find the .par parameter file.
        pars = glob.glob("%s/*.par" % in_dir)
        if not pars:
            _logger.debug("The ROI directory %s does not contain a"
                          " .par parameter file" % in_dir)
            continue
        if len(pars) > 1:
            _logger.debug("The ROI directory %s contains more than"
                          " one .par parameter file" % in_dir)
            continue
        par = pars[0]
        params = _collect_parameters(par)

        # If there is no slice number, then complain.
        slice_seq_nbr_s = params.get('CurrentSlice')
        if not slice_seq_nbr_s:
            _logger.debug("The ROI slice could not be determined from"
                          " the parameter file: %s" % param_file_name)
            continue
        slice_seq_nbr = int(slice_seq_nbr_s)

        # If there is no volume number, then complain.
        volume_nbr_s = params.get('CurrentTimePt')
        if not volume_nbr_s:
            _logger.debug("The ROI volume could not be determined from"
                          " the parameter file: %s" % param_file_name)
            continue
        volume_nbr = int(volume_nbr_s)

        yield LesionROI(lesion, volume_nbr, slice_seq_nbr, bqf)

def _collect_parameters(in_file):
    """
    Parses the parameters from the ``.par`` file.

    :param in_file: the parameter file path
    :return: the parameter {name: value} dictionary
    """
    params = {}
    with open(in_file) as f:
        lines = f.readlines()
        for line in lines:
            match = PARAM_REGEX.match(line)
            if match:
                key = match.group('key')
                value = match.group('value')
                params[key] = value
    return params
