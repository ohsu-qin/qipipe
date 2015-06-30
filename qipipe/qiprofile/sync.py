from qiprofile_rest_client.helpers import database
from qiprofile_rest_client.model.subject import Subject
from . import (clinical, imaging)


def sync_session(project, collection, subject, session, filename):
    """
    Updates the qiprofile database from the XNAT database content for
    the given session.

    :param project: the XNAT project name
    :param collection: the image collection name
    :param subject: the subject number
    :param session: the XNAT session name, without subject prefix
    :param filename: the XLS input file location
    """
    # Get or create the database subject.
    sbj_pk = dict(project=project, collection=collection, number=subject)
    sbj = database.get_or_create(Subject, sbj_pk)
    # Update the clinical information from the XLS input.
    clinical.sync(sbj, filename)
    # Update the imaging information from XNAT.
    imaging.sync(sbj, session)
