"""
This module updates the qiprofile database Subject chemotherapy protocol
information from a Chemotherapy Excel worksheet.
"""

from qiprofile_rest_client.model.clinical import (Dosage, Drug)
from .dosage import (DosageWorksheet, DosageUpdate)
from . import parse

COL_ATTRS = {'Cumulative Amount (mg/kg)': 'amount'}
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
    ChemotherapyUpdate(subject).update(rows)


class ChemotherapyError(Exception):
    pass


class ChemotherapyUpdate(DosageUpdate):
    """The subject chemotherapy update facade class."""
    
    def __init__(self, subject):
        """
        :param subject: the ``Subject`` Mongo Engine database object
            to update
        """
        super(ChemotherapyUpdate, self).__init__(subject)

    def dosage_for(self, treatment, row):
        """
        :param treatment: the target treatment
        :param row: the input row
        :return: the dosage database object which matches the agent
            name and start date, or a new dosage database object if
            there is no match
        """
        # Find the matching dosage by agent, if any.
        # If no match, then make a new dosage database object.
        dosage_iter = (dosage for dosage in treatment.dosages
                       if dosage.agent.name == row.name and
                          dosage.start_date == row.start_date)
        target = next(dosage_iter, None)
        # If no match, then make a new dosage database object.
        if not target:
            agent = Drug(name=row.name)
            target = Dosage(agent=agent)
            treatment.dosages.append(target)

        return target
