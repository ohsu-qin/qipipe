import re
from .staging_error import StagingError

__all__ = ['with_name']

EXTENT = {}
"""A name => collection dictionary for all supported AIRC collections."""

def with_name(name):
    """
    @param name: the OHSU QIN collection name
    @return: the corresponding AIRC collection
    """
    return EXTENT[collection]


class AIRCCollection(object):
    """The AIRC Study characteristics."""

    def __init__(self, collection, subject_pattern, session_pattern):
        """
        @param collection: the collection name
        @param subject_pattern: the subject directory name match pattern
        @param session_pattern: the session directory name match pattern
        """
        self.collection = collection
        self.subject_pattern = subject_pattern
        self.session_pattern = session_pattern
        EXTENT[collection] = self
    
    def path2subject_number(self, path):
        """
        @param path: the directory path
        @return: the subject number
        """
        match = re.search(self.subject_pattern, path)
        if not match:
            raise StagingError("The directory path %s does not match the subject pattern %s" % (path, self.subject_pattern))
        return int(match.group(1))
    
    def path2session_number(self, path):
        """
        @param path: the directory path
        @return: the session number
        """
        return int(re.search(self.session_pattern, path).group(1))

BREAST = AIRCCollection('Breast', 'BreastChemo(\d+)', 'Visit(\d+)')

SARCOMA = AIRCCollection('Sarcoma', 'Subj_(\d+)', '(?:Visit|S\d+V)(\d+)')