"""
This module updates the qiprofile database Subject radiation protocol
information from a Radiotherapy Excel worksheet.
"""

import datetime
from qiprofile_rest_client.model.clinical import (Dosage, Radiation)
from .dosage import (DosageWorksheet, DosageUpdate)
from . import parse

SHEET = 'Radiotherapy'
"""The input XLS sheet name."""

COL_ATTRS = {'Cumulative Amount (Gy/kg)': 'amount'}
"""
The following non-standard column-attribute associations:
* The Cumulative Amount column is the amount attribute.
"""


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
    RadiotherapyUpdate(subject).update(rows)


class RadiotherapyError(Exception):
    pass


class RadiotherapyUpdate(DosageUpdate):
    """The subject radiotherapy update facade class."""
    
    DEFAULTS = dict(beam_type='photon')
    
    def __init__(self, subject):
        """
        :param subject: the ``Subject`` Mongo Engine database object
            to update
        """
        super(RadiotherapyUpdate, self).__init__(
            subject, **RadiotherapyUpdate.DEFAULTS
        )

    def dosage_for(self, treatment, row):
        """
        :param treatment: the target treatment
        :param row: the input row
        :return: the dosage database object which matches the agent beam type,
            or a new dosage database object if there is no match
        """
        # Find the matching dosage by agent, if any.
        # If no match, then make a new dosage database object.
        dosage_iter = (dosage for dosage in treatment.dosages
                       if dosage.agent.beam_type == row.beam_type)
        target = next(dosage_iter, None)
        # If no match, then make a new dosage database object.
        if not target:
            agent = Radiation(beam_type=row.beam_type)
            target = Dosage(agent=agent)
            treatment.dosages.append(target)

        return target
