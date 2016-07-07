"""
This module updates the qiprofile database Subject radiation protocol
information from a Radiotherapy Excel worksheet.
"""

import datetime
from qirest_client.model.clinical import (Dosage, Radiation)
from .dosage import (DosageWorksheet, DosageUpdate)
from . import parse

SHEET = 'Radiotherapy'
"""The input XLS sheet name."""

COL_ATTRS = {'Cumulative Amount (Gy)': 'amount'}
"""
The following non-standard column-attribute associations:
* The Cumulative Amount column is the amount attribute.
"""

AGENT_DEFAULTS = dict(beam_type='photon')
"""The default beam type is ``photon``."""


def read(workbook, **condition):
    """
    This is a convenience method that wraps :class:`RadiotherapyWorksheet`
    :meth:`qipipe.qiprofile.xls.Worksheet.read`.
    
    :param workbook: the read-only ``openpyxl`` workbook object
    :param condition: the :meth:`qipipe.qiprofile.xls.Worksheet.read`
        filter condition
    :return: the :meth:`qipipe.qiprofile.xls.Worksheet.read` rows
    """
    reader = DosageWorksheet(workbook, SHEET, Radiation,
                             column_attributes=COL_ATTRS)
    
    return reader.read(**condition)


def update(subject, rows):
    """
    Updates the given subject data object from the dosage XLS rows.
    
    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the input radiotherapy :meth:`read` rows list
    
    """
    updater = DosageUpdate(subject, Radiation, **AGENT_DEFAULTS)
    updater.update(rows)
