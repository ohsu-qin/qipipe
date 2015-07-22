from qiprofile_rest_client.helpers import database
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.imaging import Session
from . import (clinical, imaging)


def sync_session(project, collection, subject, session, filename):
    """
    Updates the qiprofile database from the XNAT database content for
    the given session.

    :param project: the XNAT project name
    :param collection: the image collection name
    :param subject: the subject number
    :param session: the XNAT session number
    :param filename: the XLS input file location
    """
    # Get or create the subject database subject.
    key = dict(project=project, collection=collection, number=subject)
    sbj = database.get_or_create(Subject, key)
    # Update the clinical information from the XLS input.
    clinical.sync(sbj, filename)
    # Update the imaging information from XNAT.
    imaging.sync(sbj, session)
