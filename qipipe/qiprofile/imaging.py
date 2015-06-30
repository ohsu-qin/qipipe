"""
This module updates the qiprofile database imaging information
from a XNAT experiment.
"""
import qixnat
from ..staging.iterator import (SUBJECT_FMT, SESSION_FMT)
from . import modeling


class ImagingError(Exception):
    pass


def sync(subject, session_number):
    """
    Updates the imaging content for the given qiprofile subject database object
    from the XNAT experiment corresponding to the given session number.

    :param subject: the target qiprofile Subject to update
    :param session_number: the number of the new session
    """
    # The XNAT subject name.
    sbj_name = SUBJECT_FMT % (subject.collection, subject.number)
    # The XNAT experiment name.
    exp_name = SESSION_FMT % session.number
    
    # Make the qiprofile session object.
    has_session_with_number = lambda sess: sess.number == session_number
    if any(has_session_with_number, subject.sessions):
        raise ImagingError(
            "%s %s Subject %d Session %d already exists in the qiprofile"
            " database" % (subject.project, subject.collection, subject.number,
                           session.number)
    )
    session = Session(number=session_number)
    
    # Connect to XNAT.
    with qixnat.connect() as xnat:
        # The XNAT experiment.
        exp = xnat.get_experiment(subject.project, sbj_name, exp_name)
        if not exp.exists():
            raise ImagingError(
                "%s %s Subject %d Session %d XNAT experiment not found" %
                (subject.project, subject.collection, subject.number,
                 session.number)
            )
        # Update the qiprofile session from the XNAT experiment.
        _update(session, exp)


def _update(session, experiment):
    """
    Updates the qiprofile session from the XNAT experiment.
    
    :param session: the qiprofile session object
    :param experiment: the XNAT experiment object
    """
    # The modeling is embedded in the Subject document's session object.
    for rsc in experiment.resources():
        if rsc.label.startswith('pk_'):
            modeling.update(session, rsc)
    # The scans are embedded in the SessionDetail document.
    for scan in experiment.scans():
        scan.update(session, scan)
