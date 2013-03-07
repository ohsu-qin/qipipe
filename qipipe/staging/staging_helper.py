import os
import re
from .staging_error import StagingError

_NPAT = re.compile('^subject(\d+)$')

def extract_subject_number_from_path(path):
    """
    Extracts the subject number from the given image file path.
    The path must contain a subject directory which matches C{^subject(\d+)$}.
    
    @param path: the path within a subject<nn> directory
    @return the subject number
    @raise StagingError: if subject directory was not found in the path
    """
    for d in path.split(os.path.sep):
        match = _NPAT.match(d)
        if match:
            return int(match.group(1))
    raise StagingError('A subject directory was not found in the path: ' + path)
    