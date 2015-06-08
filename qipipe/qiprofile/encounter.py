"""
This module updates the qiprofile database Subject encounter
information from the encounter Excel workbook file.
"""

from bunch import Bunch
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import Encounter
from . import xls
from . import parsers

PARSERS = dict(encounter_type=lambda s: s.capitalize())
"""The encounter type is capitalized."""


class EncounterError(Exception):
    pass


def filter(filename, subject):
    """
    Finds the encounter XLS row which matches the given subject.

    :param filename: the Excel workbook file location
    :param subject: the XNAT subject name
    :return: the encounter :meth:`qipipe.qiprofile.xls.filter` rows list
    :raise XLSError: if no XLS row matched the subject
    """
    opts = parsers.default_parsers(Encounter)
    opts.update(PARSERS)
    reader = xls.Reader(filename, 'Encounter', **opts)

    return list(reader.filter(subject))


def update(subject, rows):
    """
    Updates the given subject data object from the encounter XLS rows.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the encounter :meth:`filter` rows list 
    """
    for row in rows:
        _update(subject, row)

def _update(subject, row):
    """
    Updates the given subject data object from the encounter input.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param row: the input encounter :meth:`update` row
    """
    # Look for a matching encounter.
    target = next((trt for trt in subject.encounters
                   if trt.start_date == row.start_date
                   and trt.end_date == row.end_date),
                  None)
    # If there is not a match, then make a new encounter database object.
    if not target:
        # Validate that this encounter does not overlap with an existing
        # encounter object.
        for trt in subject.encounters:
            if _is_overlapping(row, trt):
                row_end_date_s = row.end_date.date() if row.end_date else '...'
                trt_end_date_s = trt.end_date.date() if trt.end_date else '...'
                raise EncounterError("The input encounter XLS row extent"
                                     "[%s, %s] lies within an existing"
                                     " encounter extent [%s, %s]" %
                                     (row.start_date.date(), row_end_date_s,
                                      trt.start_date.date(), trt_end_date_s))
        # Make the encounter database object.
        target = Encounter()
        subject.encounters.append(target)

    # Update the target dosage database object.
    for attr, val in row.iteritems():
        setattr(target, attr, val)


def _is_overlapping(t1, t2):
    """
    :return: whether the two encounters overlap
    """
    return _is_within(t1.start_date, t2) or _is_within(t1.end_date, t2)


def _is_within(date, encounter):
    """
    :param date: the date to check
    :param encounter: the encounter to check
    :return: whether the date lies within the encounter bounds
    """
    return (date and date >= encounter.start_date
            and (not encounter.end_date or date <= encounter.end_date))
