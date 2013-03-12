import re

__all__ = ['for_collection']

EXTENT = {}
"""A collection => study dictionary for all supported AIRC studies."""

def for_collection(collection):
    """
    @param collection: the OHSU QIN collection name
    @return: the corresponding AIRC study
    """
    return EXTENT[collection]


class AIRCStudy(object):
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
        return int(re.search(self.subject_pattern, path).group(1))
    
    def path2session_number(self, path):
        """
        @param path: the directory path
        @return: the session number
        """
        return int(re.search(self.session_pattern, path).group(1))
        

BREAST = AIRCStudy('Breast', 'BreastChemo(\d+)', 'Visit(\d+)')

SARCOMA = AIRCStudy('Sarcoma', 'Subj_(\d+)', '(?:Visit|S\d+V)(\d+)')