"""
This module updates the qiprofile database Subject demographics
information from the demographics Excel workbook file.
"""

import re
from qirest_client.model.subject import Subject
from .xls import Worksheet
from . import parse

SHEET = 'Demographics'
"""The input XLS sheet name."""

COL_ATTRS = {'Race': 'races'}
"""
The following non-standard column-attribute associations:
* The Race column is the races attribute.
"""


class DemographicsError(Exception):
    pass


def read(workbook, **condition):
    """
    Reads the demographics XLS row which matches the given subject.
    
    :param condition: the row selection filter
    :return: the Demographics sheet
        :meth:`qipipe.qiprofile.xls.Worksheet.read`
        row bundle which matches the given subject, or None if no
        match was found
    """
    # Wrap the worksheet to read Subject attributes.
    reader = Worksheet(workbook, SHEET, Subject, column_attributes=COL_ATTRS)
    
    # The filtered worksheet rows.
    return reader.read(**condition)


def update(subject, rows):
    """
    Updates the given subject data object from the given Demographics
    sheet rows.
    
    There can be no more than one Demographics update input row for
    the given subject. The *rows* parameter is an iterable in order to
    conform to other sheet facade modules.
    
    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the input Demographics :meth:`read` rows
    :raise DemographicsError: if there is more than one input row
    """
    row_list = list(rows)
    # An empty row is valid and is a no-op.
    # More than one row is an error.
    if not row_list:
        return
    elif len(row_list) > 1:
        raise DemographicsError("Subject number %d has more than one row" %
                                 subject.number)
    # Use the single row.
    row = row_list[0]
    for attr, val in row.iteritems():
        if hasattr(subject, attr):
            setattr(subject, attr, val)
