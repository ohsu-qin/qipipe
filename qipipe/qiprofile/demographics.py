"""
This module updates the qiprofile database Subject demographics
information from the demographics Excel workbook file.
"""

import re
from qiprofile_rest_client.model.subject import Subject
from . import xls
from . import parsers


class DemographicsError(Exception):
    pass


def filter(filename, subject):
    """
    Finds the demographics XLS row which matches the given subject.
    
    :param filename: the Excel workbook file location
    :param subject: the XNAT subject name
    :return: the :meth:`qipipe.qiprofile.xls.filter` subject
        demographics row, or None if no match was found
    :raise DemographicsError: if more than one XLS row matches
        the subject
    """
    opts = parsers.default_parsers(Subject)
    reader = xls.Reader(filename, 'Demographics', **opts)
    rows = list(reader.filter(subject))
    if len(rows) > 1:
        sbj_nbr = parsers.parse_trailing_number(self.subject)
        raise DemographicsError("Subject number %d has more than one row in"
                                " the Excel workbook file %s" %
                                (sbj_nbr, filename))

    return rows[0] if rows else None


def update(subject, row):
    """
    Updates the given subject data object from the demographics XLS row.

    :param subject: the ``Subject`` database object to update
    :param row: the input demographics {attribute: value} dictionary
    """
    for attr, val in row.iteritems():
        setattr(subject, attr, val)
