import re
from .staging_error import StagingError
from . import collections

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


class Collection(object):
    """The image collection."""

    def __init__(self, name, **opts):
        """
        :param name: `self.name`
        :param opts: the :class:`Patterns` attributes
        """
        self.name = name
        """The collection name."""

        self.patterns = Patterns(**opts)
        """The file and DICOM meta-data patterns."""

        # Add this collection to the collections extent.
        collections.add(self)