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


PARAM_REGEX = re.compile('(?P<key>\w+)\s*\:\s*(?P<value>\w+)')
"""
The regex to parse a parameter file.
"""


def iter_roi(glob_pat, regex, input_dir):
    """
    Iterates over the the OHSU ROI ``.bqf`` mask files in the given
    input directory. This method is a :class:`LesionROI` generator,
    e.g.::

        >>> # Find .bqf files anywhere under /path/to/session/processing.
        >>> next(iter_roi('processing/*', '.*/\.bqf', '/path/to/session'))
        {lesion: 1, slice: 12, path: '/path/to/session/processing/rois/roi.bqf'}

    :param glob_pat: the glob match pattern
    :;param regex: the file name match regular expression
    :param input_dir: the source session directory to search
    :yield: the :class:`LesionROI` objects
    """
    finder = qiutil.file.Finder(glob_pat, regex)
    for match in finder.match(input_dir):
        file_name = match.group(0)
        # If there is no lesion qualifier, then there is only one lesion.
        try:
            lesion_s = match.group('lesion')
        except IndexError:
            lesion_s = None
        lesion = int(lesion_s) if lesion_s else 1

        # Prepend the full input directory to the file match.
        abs_input_dir = os.path.abspath(input_dir)
        file_path = '/'.join([abs_input_dir, file_name])
        # The volume and slice are in the companion parameters file.
        roi_dir, _ = os.path.split(file_path)
        param_file_pat = "%s/*.par" % roi_dir
        param_file_names = glob.glob(param_file_pat)
        if not param_file_names:
            raise ROIError("The ROI slice directory does not have"
                           " a parameter file: %s" % roi_dir)
        if len(param_file_names) > 1:
            raise ROIError("The ROI slice directory has more than"
                           "one parameter file: %s" % roi_dir)
        param_file_name = param_file_names[0]
        params = _collect_parameters(param_file_name)

        # If there is no slice number, then complain.
        slice_seq_nbr_s = params.get('CurrentSlice')
        if not slice_seq_nbr_s:
            raise ROIError("The ROI slice could not be determined from"
                           " the parameter file: %s" % param_file_name)
        slice_seq_nbr = int(slice_seq_nbr_s)

        # If there is no volume number, then complain.
        volume_nbr_s = params.get('CurrentTimePt')
        if not volume_nbr_s:
            raise ROIError("The ROI volume could not be determined from"
                           " the parameter file: %s" % param_file_name)
        volume_nbr = int(volume_nbr_s)

        yield LesionROI(lesion, volume_nbr, slice_seq_nbr, file_path)

def _collect_parameters(param_file_name):
    """
    Parses the parameters from the ``.par`` file.

    :param param_file_name: the paramter file location
    :return: the parameter {name: value} dictionary
    """
    params = {}
    with open(param_file_name) as f:
        lines = f.readlines()
        for line in lines:
            match = PARAM_REGEX.match(line)
            if match:
                key = match.group('key')
                value = match.group('value')
                params[key] = value
    return params
