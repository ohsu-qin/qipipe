"""
This module updates the qiprofile database imaging information
from a XNAT scan.
"""
from qiutil.file import splitexts
from qiprofile_rest_client.helpers import database
from qiprofile_rest_client.model.imaging import (Session, SessionDetail,
                                                 Modeling, ParameterResult)
from ..helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from ..pipeline.modeling import FASTFIT_PARAMS_FILE
from ..pipeline.modeling import INFERRED_R1_0_OUTPUTS as OUTPUTS
from . import modeling


class ImagingError(Exception):
    pass


def update(subject, scan):
    """
    Updates the imaging content for the given qiprofile REST Subject
    from the given XNAT scan.

    :param subject: the target qiprofile Subject to update
    :param scan: the XNAT scan object
    """
    # The parent XNAT experiment.
    exp = scan.parent()
    # The XNAT experiment must have a date.
    date = exp.attrs.get('date')
    if not date:
        raise ImagingError(
            "The XNAT %s %s Subject %d %s experiment is missing"
            " the visit date" % (subject.project, subject.collection,
                         subject.number, exp.label)
        )
    # If there is a qiprofile session with the same date,
    # then complain.
    if any(sess.date == date for sess in subject.sessions):
        raise ImagingError(
            "qiprofile %s %s Subject %d session with visit date %s"
            " already exists" % (subject.project, subject.collection,
                                 subject.number, date)
        )
    # Make the qiprofile session database object.
    session = _create_session(exp)
    # Add the session to the subject encounters in date order.
    subject.add_encounter(session)
    # Save the session detail.
    session.detail.save()
    # Save the subject.
    subject.save()


def _create_session(xnat_exp):
    """
    Makes the qiprofile Session object from the XNAT scan.
    
    :param xnat_exp: the XNAT experiment object
    :return: the qiprofile session object
    """
    # Make the qiprofile scans.
    scans = [_create_scan(xnat_scan) for xnat_scan in xnat_exp.scans()]

    # The modeling resources begin with 'pk_'.
    xnat_mdl_rscs = (rsc for rsc in xnat_scan.resources()
                     if rsc.label().startswith('pk_'))
    modelings = [_create_modeling(rsc) for rsc in xnat_mdl_rscs]

    # The session detail database object to hold the scans.
    detail = SessionDetail(scans=scans)
    # Save the detail first, since it is not embedded and we need to
    # set the detail reference to make the session.
    detail.save()

    # The XNAT experiment date is the qiprofile session date.
    date = xnat_exp.attrs.get('date')

    # Return the new qiprofile Session object.
    return Session(date=date, modelings=modelings, detail=detail)


def _create_scan(xnat_scan):
    """
    Makes the qiprofile Session object from the XNAT scan.
    
    :param xnat_scan: the XNAT scan object
    :return: the qiprofile scan object
    """
    # The modeling resources begin with 'pk_'.
    xnat_mdl_rscs = (rsc for rsc in xnat_scan.resources()
                     if rsc.label().startswith('pk_'))
    modelings = [_create_modeling(rsc) for rsc in xnat_mdl_rscs]

    # The qiprofile scan to embed in the SessionDetail document.
    for scan in scan.scans():
        _update_scan(session, scan)
    # The session detail database object to hold the scans.
    detail = SessionDetail(scans=[scan])
    # Save the detail first, since it is not embedded and we need to
    # set the detail reference to make the session.
    detail.save()
    
    return Session(date=date, modelings=modelings,
                   tumor_extents=tumor_extents, detail=detail)


def _create_modeling(xnat_resource):
    """
    Creates the qiprofile Modeling object from the given XNAT
    resource object.

    :param xnat_resource: the XNAT modeling resource object
    """
    # The XNAT modeling files.
    xnat_files = xnat_resource.files()
    
    # The fastfit parameters.
    fastfit_finder = (xnat_file for xnat_file in xnat_files
                        if xnat_file.label() == FASTFIT_PARAMS_FILE)
    xnat_fastfit_file = next(fastfit_finder, None)
    if not xnat_fastfit_file:
        raise ImagingError("The XNAT modeling resource %s does not contain"
                           " input parameter file %s" %
                           (xnat_resource.label(), FASTFIT_PARAMS_FILE))
    fastfit_location = xnat_fastfit_file.get()
    with open(fastfit_location) as fastfit_file:
        csv_reader = csv.reader(fastfit_file)
        fastfit_dict = {row[0], ','.join(row[1:]) for row in csv_reader}
    
    # The qiprofile modeling output files.
    possible_output_files = {output + '.nii.gz' for output in OUTPUTS}
    output_xnat_files = (xnat_file.label() for xnat_file in xnat_files
                         if xnat_file.label() in possible_output_files)
    

def _update_scan(session, xnat_scan):
    """
    Updates the scan content for the given qiprofile session
    database object from the given XNAT scan object.

    :param session: the target qiprofile Session object to update
    :param xnat_scan: the XNAT scan object
    """
    # TODO
    pass
