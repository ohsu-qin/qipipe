import os
import re
from .staging_error import StagingError

_NPAT = re.compile('^patient(\d+)$')

def extract_patient_number_from_path(path):
    """
    Extracts the patient number from the given image file path.
    The path must contain a patient directory which matches C{^patient(\d+)$}.
    
    @param path: the path within a patient<nn> directory
    @return the patient number
    @raise StagingError: if patient directory was not found in the path
    """
    for d in path.split(os.path.sep):
        match = _NPAT.match(d)
        if match:
            return int(match.group(1))
    raise StagingError('A patient directory was not found in the path: ' + path)
    