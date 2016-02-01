import re
from .staging_error import StagingError


class ROIPatterns(object):
    """The ROI file name matching patterns."""
    
    def __init__(self, glob, regex):
        self.glob = glob
        """The ROI file glob pattern."""
        
        self.regex = regex
        """The ROI file name match regular expression."""
    
    def __repr__(self):
        return str(dict(glob=self.glob, regex=self.regex))


class ScanPatterns(object):
    """The scan file name matching patterns."""
    
    def __init__(self, dicom, roi=None):
        self.dicom = dicom
        """The DICOM file match *glob* and *regex* patterns."""
        
        self.roi = roi
        """The :class:`ROIPatterns` object."""
    
    def __repr__(self):
        return str(dict(dicom=self.dicom, roi=self.roi))


class Patterns(object):
    """The collection file name and DICOM tag patterns."""
    
    def __init__(self, subject, session, scan, volume):
        """
        :param subject: the subject directory name match regular expression
        :param session: the session directory name match regular expression
        :param scan the {scan number: :class:`ScanPatterns} dictionary
        :option volume: the DICOM tag which identifies a scan volume
        """
        self.subject = subject
        """The subject directory match pattern."""
        
        self.session = session
        """The subject directory match pattern."""
        
        self.scan = scan
        """The {scan number: :class:`ScanPatterns`} dictionary."""
        
        self.volume = volume
        """The DICOM tag which identifies a scan volume."""


def with_name(name):
    """
    :return: the :class:`Collection` whose name is a case-insensitive
        match for the given name, or None if no match is found
    """
    return Collection.instances.get(name.lower())


class Collection(object):
    """The image collection."""
    
    instances = {}
    """The collection {name: object} dictionary."""
    
    def __init__(self, name, **opts):
        """
        :param name: `self.name`
        :param opts: the :class:`Patterns` attributes as well as the
            following keyword options:
        :keyword crop_posterior: the :attr:`crop_posterior` flag
        """
        self.name = name.capitalize()
        """The capitalized collection name."""
        
        self.crop_posterior = opts.pop('crop_posterior', False)
        """
        A flag indicating whether to crop the image posterior in the
        mask, e.g. for a breast tumor (default False).
        """
        
        self.scan_types = opts.pop('scan_types', {})
        """
        The scan {number: type} dictionary.
        """
        
        self.patterns = Patterns(**opts)
        """The file and DICOM meta-data patterns."""
        
        # The scan pattern key is the scan number.
        # There must be a scan type for each number.
        if self.patterns.scan:
            for scan_number in self.patterns.scan:
                if not scan_number in self.scan_types:
                    raise StagingError("The scan number %d is missing a"
                                       " scan type" % scan_number)
        
        # If a collection with this name has not yet been recorded,
        # then add this canonical instance to the collection extent.
        key = name.lower()
        if key not in Collection.instances:
            Collection.instances[key] = self
