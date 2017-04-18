from qirest_client.helpers import database
from qirest_client.model.subject import Subject
from qirest_client.model.imaging import Session
from . import (clinical, imaging)


def update(project, collection, subject, session, spreadsheet):
    """
    Updates the qiprofile database from the clinical spreadsheet and
    XNAT database for the given session.
    
    :param project: the XNAT project name
    :param collection: the image collection name
    :param subject: the subject number
    :param session: the XNAT session number
    :param spreadsheet: the spreadsheet input file location
    :param modeling_technique: the modeling technique
    """
    # Get or create the subject database subject.
    key = dict(project=project, collection=collection, number=subject)
    sbj = database.get_or_create(Subject, key)
    # Update the clinical information from the XLS input.
    clinical.update(sbj, spreadsheet)
    # Update the imaging information from XNAT.
    imaging.update(sbj, session)
