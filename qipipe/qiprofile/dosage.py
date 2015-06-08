"""
This module updates the qiprofile database Subject dosage information
from the dosage Excel workbook file.
"""

from qiprofile_rest_client.model.uom import (Measurement, Weight)
from qiprofile_rest_client.model.clinical import (Dosage, Drug, Radiation)
from . import xls
from . import parsers

PARSERS = dict(agent=lambda s: s.lower())
"""The agent is lower case."""


class DosageError(Exception):
    pass


def filter(filename, subject):
    """
    Finds the dosage XLS row which matches the given subject.

    :param filename: the Excel workbook file location
    :param subject: the XNAT subject name
    :return: the dosage :meth:`qipipe.qiprofile.xls.filter` rows list
    """
    opts = parsers.default_parsers(Dosage)
    opts.update(PARSERS)
    reader = xls.Reader(filename, 'Dosage', **opts)

    return list(reader.filter(subject))


def update(subject, rows):
    """
    Updates the given subject data object from the dosage XLS rows.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the dosage :meth:`filter` rows list 
    
    """
    for row in rows:
        _update(subject, row)


def _update(subject, row):
    """
    Updates the given subject data object from the dosage input.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param row: the input dosage :meth:`update` row
    :raise DosageError: if the row dates are not contained within
        exactly one treatment
    """
    # Find the spanning treatment object.
    tgt_trt = _treatment_spanning(subject, row.start_date)
    # Find or make the target dosage object.
    target = _dosage_for(tgt_trt, row.agent)
    # Collect the update attributes.
    attrs = (attr for attr in Dosage._fields if attr in row)
    # Update the target dosage database object.
    for attr in attrs:
        setattr(target, attr, row[attr])


def _dosage_for(treatment, agent):
    """
    :param treatment: the target treatment
    :param agent: the input agent
    :return: the dosage database object which matches the agent,
        or a new dosage database object if there is no match
    """
    # Find the matching dosage by agent, if any.
    # If no match, then make a new dosage database object.
    target = next((d for d in treatment.dosages if d.agent == agent),
                  None)
    # If no match, then make a new dosage database object.
    if not target:
        tgt_type = Radiation if agent in Radiation.FORMS else Drug
        target = tgt_type(agent=agent)
        treatment.dosages.append(target)

    return target


def _treatment_spanning(subject, start_date):
    """
    :param subject: the subject database object
    :param start_date: the dosage start date
    :raise DosageError: if the row date is not contained within
        exactly one treatment
    """
    # Find the candidate treatments.
    trts = [trt for trt in subject.treatments
            if trt.start_date <= start_date
            and start_date <= trt.end_date]
    # There must be a unique candidate.
    if not trts:
        raise DosageError("No treatment was found which spans the dosage"
                          " start date %s" % start_date.date)
    if len(trts) > 1:
        raise DosageError("More than one treatment was found which spans"
                          " the dosage start date %s" % start_date.date)

    # Return the target treatment.
    return trts[0]
