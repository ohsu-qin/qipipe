"""
This module updates the qiprofile database Subject chemotherapy protocol
information from a Chemotherapy Excel worksheet.
"""

from qirest_client.model.clinical import (Dosage, Drug)
from .dosage import (DosageWorksheet, DosageUpdate)
from . import parse

COL_ATTRS = {'Cumulative Amount (mg/m2 BSA)': 'amount'}
"""
The following non-standard column-attribute associations:
* The Cumulative Amount column is the amount attribute.
"""

SHEET = 'Chemotherapy'
"""The input XLS sheet name."""


def read(workbook, **condition):
    """
    This is a convenience method that wraps :class:`ChemotherapyWorksheet`
    :meth:`qipipe.qiprofile.xls.Worksheet.read`.
    
    :param workbook: the read-only ``openpyxl`` workbook object
    :param condition: the :meth:`qipipe.qiprofile.xls.Worksheet.read`
        filter condition
    :return: the :meth:`qipipe.qiprofile.xls.Worksheet.read` rows
    """
    reader = DosageWorksheet(workbook, SHEET, Drug,
                             column_attributes=COL_ATTRS)
    
    return reader.read(**condition)


def update(subject, rows):
    """
    Updates the given subject data object from the dosage XLS rows.
    
    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the input chemotherapy :meth:`read` rows list
    
    """
    updater = DosageUpdate(subject, Drug)
    updater.update(rows)
