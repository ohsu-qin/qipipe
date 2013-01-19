import os
import re
from .staging_error import StagingError

_NPAT = re.compile('\d+$')

def extract_trailing_integer_from_path(path):
    """
    Extracts the trailing number from the given file path.
    
    @param path: the path with a trailing integer
    @return the patient number
    @raise StagingError: if the path does not end in an integer
    """
    match = _NPAT.search(os.path.basename(path))
    if not match:
        raise StagingError('The source directory does not end in a number: ' + path)
    return int(match.group(0))
    