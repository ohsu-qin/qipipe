import re
from .staging_error import StagingError

def collection_with_name(name):
    """
    :param name: the OHSU QIN collection name
    :return: the corresponding AIRC collection
    :raise ValueError: if the given collection name is not recognized
    """
    if not hasattr(collection_with_name, 'extent'):
        setattr(collection_with_name, extent, _create_collections())
    if name not in collection_with_name.extent:
        raise ValueError("The AIRC collection name is not recognized: %s" % name)
    
    return collection_with_name.extent[name]

def _create_collections():
    """Creates the pre-defined AIRC collections."""
    
    return dict(
        Breast=AIRCCollection('Breast', 'BreastChemo(\d+)', 'Visit(\d+)', '*concat*/*'),
        Sarcoma=AIRCCollection('Sarcoma', 'Subj_(\d+)', '(?:Visit_|S\d+V)(\d+)', '*concat*/*')
    )


class AIRCCollection(object):
    """The AIRC Study characteristics."""
    
    
    def __init__(self, name, subject_pattern, session_pattern, dicom_pattern):
        """
        :param name: `self.name`
        :param subject_pattern: `self.subject_pattern`
        :param session_pattern: `self.session_pattern`
        :param dicom_pattern: `self.dicom_pattern`
        """
        self.name = name
        """The collection name."""
        
        self.subject_pattern = subject_pattern
        """The subject directory name match regular expression pattern."""
        
        self.session_pattern = session_pattern
        """The session directory name match regular expression pattern."""
        
        self.dicom_pattern = dicom_pattern
        """The DICOM directory name match glob pattern."""
    
    def path2subject_number(self, path):
        """
        :param path: the directory path
        :return: the subject number
        :raise StagingError: if the path does not match the collection subject pattern
        """
        match = re.search(self.subject_pattern, path)
        if not match:
            raise StagingError("The directory path %s does not match the subject pattern %s" % (path, self.subject_pattern))
        
        return int(match.group(1))
    
    def path2session_number(self, path):
        """
        :param path: the directory path
        :return: the session number
        :raise StagingError: if the path does not match the collection session pattern
        """
        match = re.search(self.session_pattern, path)
        if not match:
            raise StagingError("The directory path %s does not match the session pattern %s" % (path, self.session_pattern))
        return int(match.group(1))
