"""
This module updates the qiprofile database imaging information
from a XNAT experiment.
"""
import qixnat
from ..helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from . import modeling


class ImagingError(Exception):
    pass


def sync(subject, session):
    """
    Updates the imaging content for the given qiprofile subject database
    object from the given XNAT experiment.

    :param subject: the target qiprofile subject object to update
    :param session: the session number
    """
    # Sync the content in a XNAT connection context.
    with qixnat.connect() as xnat:
        # The XNAT subject name.
        sbj_nm = SUBJECT_FMT % (collection, subject)
        # The XNAT experiment name.
        exp_nm = SESSION_FMT % session
        # Find the XNAT experiment
        exp = xnat.find_one(project=project, subject=sbj_nm,
                                  experiment=exp_nm)
        if not exp.exists():
            raise ImagingError(
                "%s %s Subject %d Session %d XNAT experiment not found" %
                (subject.project, subject.collection, subject.number,
                 session.number)
            )
        # The XNAT experiment must have a date.
        if not exp.date:
            raise ImagingError(
                "%s %s Subject %d Session %d XNAT experiment is missing"
                " a date" % (subject.project, subject.collection,
                             subject.number, session.number)
            )
        # If there is a qiprofile session with the same date,
        # then complain.
        is_dup_session = lambda sess: sess.date == exp.date
        if any(is_dup_session, sbj.sessions):
            raise ImagingError(
                "qiprofile %s %s Subject %d session with date %s already"
                " exists" % (subject.project, subject.collection,
                             subject.number, exp.date)
            )
        # Make the qiprofile session database object.
        sess = Session(date=exp.date)
        # Add the session to the subject encounters in date order.
        sbj.add_encounter(sess)
        # Update the qiprofile session object.
        _update(sess, exp)
        # Save the session detail.
        sess.detail.save()
        # Save the subject.
        sbj.save()

def _update(session, experiment):
    """
    Updates the qiprofile session from the XNAT experiment.
    
    :param session: the qiprofile session object
    :param experiment: the XNAT experiment object
    """
    # The modeling resources begin with 'pk+'.
    for xrsc in experiment.resources():
        if xrsc.label.startswith('pk_'):
            modeling.update(session, xrsc)
    
    # Create the session detail database subject to hold the scans.
    session.detail = database.get_or_create(SubjectDetail)
    # The scans are embedded in the SessionDetail document.
    for xscan in experiment.scans():
        scan.update(session.detail, xscan)
