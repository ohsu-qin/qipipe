"""
This module updates the qiprofile database imaging information
from a XNAT experiment.
"""
from ..helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from . import modeling


class ImagingError(Exception):
    pass


def sync(subject, experiment):
    """
    Updates the imaging content for the given qiprofile REST Subject
    from the given XNAT experiment.

    :param subject: the target qiprofile Subject to update
    :param experiment: the XNAT experiment object
    """
    # The XNAT experiment must have a date.
    if not experiment.date:
        raise ImagingError(
            "%s %s Subject %d Session %d XNAT experiment is missing"
            " the visit date" % (subject.project, subject.collection,
                         subject.number, session.number)
        )
    # If there is a qiprofile session with the same date,
    # then complain.
    is_dup_session = lambda sess: sess.date == experiment.date
    if any(is_dup_session, subject.sessions):
        raise ImagingError(
            "qiprofile %s %s Subject %d session with visit date %s"
            " already exists" % (subject.project, subject.collection,
                                 subject.number, experiment.date)
        )
    # Make the qiprofile session database object.
    session = Session(date=experiment.date)
    # Add the session to the subject encounters in date order.
    subject.add_encounter(session)
    # Update the qiprofile session object.
    _update(session, experiment)
    # Save the session detail.
    session.detail.save()
    # Save the subject.
    subject.save()


def _update(session, experiment):
    """
    Updates the qiprofile session from the XNAT experiment.
    
    :param session: the qiprofile session object
    :param experiment: the XNAT experiment object
    """
    # The modeling resources begin with 'pk_'.
    for rsc in experiment.resources():
        if rsc.label.startswith('pk_'):
            _update_modeling(session, rsc)

    # Create the session detail database subject to hold the scans.
    session.detail = database.get_or_create(SubjectDetail)
    # The scans are embedded in the SessionDetail document.
    for scan in experiment.scans():
        _update_scan(session, scan)


def _update_modeling(session, resource):
    """
    Updates the modeling content for the given qiprofile session
    database object from the given XNAT modeling resource object.

    :param session: the target qiprofile Session object to update
    :param resource: the XNAT modeling resource object
    """
    # TODO
    pass


def _update_scan(session, scan):
    """
    Updates the scan content for the given qiprofile session
    database object from the given XNAT scan object.

    :param session: the target qiprofile Session object to update
    :param scan: the XNAT scan object
    """
    # TODO
    pass
