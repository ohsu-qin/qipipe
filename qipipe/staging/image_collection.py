import re
from bunch import Bunch
from .staging_error import StagingError


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
        :param name: the :attr:`name`
        :param opts: the following keyword options:
        :option subject: the subject directory name match regular expression
        :option session: the session directory name match regular expression
        :option scan_types: the :attr:`scan_types`
        :option scan: the {scan number: {dicom, roi}} dictionary
        :option volume: the DICOM tag which identifies a scan volume
        :option crop_posterior: the :attr:`crop_posterior` flag
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

        self.patterns = Bunch(**opts)
        """
        The DICOM and ROI meta-data patterns. This ``patterns``
        attribute consists of the entries ``dicom`` and ``roi``,
        Each of these fields has a mandatory ``glob`` entry and
        an optional ``regex`` entry. The ``glob`` entry matches
        the scan subdirectory containing the DICOM or ROI files.
        The ``regex`` entry matches the DICOM or ROI files in the
        subdirectory. The default in the absence of a ``regex``
        entry is to include all files in the subdirectory.
        """

        # The scan pattern key is the scan number.
        if self.patterns.scan:
            for scan, scan_pats in self.patterns.scan.iteritems():
                # There must be a scan type for each scan.
                if not scan in self.scan_types:
                    raise StagingError("The scan %d is missing a scan"
                                       " type" % scan)
                # There must be a DICOM directory pattern
                # for each scan.
                if not scan_pats.dicom:
                    raise StagingError("The scan %d is missing a scan"
                                       " DICOM directory pattern" % scan)

        # If a collection with this name has not yet been recorded,
        # then add this canonical instance to the collection extent.
        key = name.lower()
        if key not in Collection.instances:
            Collection.instances[key] = self
