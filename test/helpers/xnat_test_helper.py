import os, re
from base64 import b64encode as encode
from qipipe.staging import airc_collection as airc
from qipipe.staging.staging_helper import SUBJECT_FMT
from qipipe.helpers import xnat_helper
from qipipe.helpers.logging_helper import logger


def generate_subject_name(name):
    """
    Makes a subject name that is unique to the given test name.
    
    :param name: the test name
    :return: the test subject name
    """
    return encode(name).strip('=')

def delete_subjects(project, *subject_names):
    """
    Deletes the given XNAT subjects, if they exist.

    :param project: the XNAT project id
    :param subject_names: the XNAT subject names
    """
    with xnat_helper.connection() as xnat:
        for sbj_lbl in subject_names:
            sbj = xnat.get_subject(project, sbj_lbl)
            if sbj.exists():
                sbj.delete()
                logger(__name__).debug("Deleted the XNAT test subject %s." % sbj_lbl)
