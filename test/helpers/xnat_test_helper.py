import os, re
from base64 import b64encode as encode
from qipipe.staging import airc_collection as airc
from qipipe.staging.staging_helper import SUBJECT_FMT
from qipipe.helpers import xnat_helper
from test.helpers.logging_helper import logger


def generate_subject_name(name):
    """
    Makes a subject name that is unique to the given test name.
    
    :param name: the test name
    :return: the test subject name
    """
    return encode(name).strip('=')
