"""
This module updates the qiprofile database Subject treatment
information from the treatment Excel workbook file.
"""

from bunch import Bunch
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import Treatment
from . import xls
from . import parsers

PARSERS = dict(treatment_type=lambda s: s.capitalize())
"""The treatment type is capitalized."""


class TreatmentError(Exception):
    pass


def filter(filename, subject):
    """
    Finds the treatment XLS row which matches the given subject.

    :param filename: the Excel workbook file location
    :param subject: the XNAT subject name
    :return: the treatment :meth:`qipipe.qiprofile.xls.filter` rows list
    :raise XLSError: if no XLS row matched the subject
    """
    opts = parsers.default_parsers(Treatment)
    opts.update(PARSERS)
    reader = xls.Reader(filename, 'Treatment', **opts)

    return list(reader.filter(subject))


def update(subject, rows):
    """
    Updates the given subject data object from the treatment XLS rows.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the treatment :meth:`filter` rows list 
    """
    for row in rows:
        _update(subject, row)


def _update(subject, row):
    """
    Updates the given subject data object from the treatment input.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param row: the input treatment :meth:`update` row
    """
    # Look for a matching treatment.
    target = next((trt for trt in subject.treatments
                   if trt.start_date == row.start_date
                   and trt.end_date == row.end_date),
                  None)
    # If there is not a match, then make a new treatment database object.
    if not target:
        # Validate that this treatment does not overlap with an existing
        # treatment object.
        for trt in subject.treatments:
            if _is_overlapping(row, trt):
                row_end_date_s = row.end_date.date() if row.end_date else '...'
                trt_end_date_s = trt.end_date.date() if trt.end_date else '...'
                raise TreatmentError("The input treatment XLS row extent"
                                     "[%s, %s] lies within an existing"
                                     " treatment extent [%s, %s]" %
                                     (row.start_date.date(), row_end_date_s,
                                      trt.start_date.date(), trt_end_date_s))
        # Make the treatment database object.
        target = Treatment()
        subject.treatments.append(target)


    # Update the target dosage database object.
    for attr, val in row.iteritems():
        setattr(target, attr, val)


def _is_overlapping(t1, t2):
    """
    :return: whether the two treatments overlap
    """
    return _is_within(t1.start_date, t2) or _is_within(t1.end_date, t2)


def _is_within(date, treatment):
    """
    :param date: the date to check
    :param treatment: the treatment to check
    :return: whether the date lies within the treatment bounds
    """
    return (date and date >= treatment.start_date
            and (not treatment.end_date or date <= treatment.end_date))
