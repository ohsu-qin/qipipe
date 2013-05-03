import os, re
from base64 import b64encode as encode
from qipipe.staging import airc_collection as airc
from qipipe.staging.staging_helper import SUBJECT_FMT
from qipipe.helpers import xnat_helper

import logging
logger = logging.getLogger(__name__)

def generate_subject_name(name):
    """
    Makes a subject name that is unique to the given test name.
    
    @param name: the test name
    @return: the test subject name
    """
    
    return 'Test_' + encode(name).strip('=')

def get_subjects(collection, source, pattern=None):
    """
    Infers the XNAT subject names from the given source directory.
    The source directory contains subject subdirectories.
    The match pattern matches on the subdirectories and captures the
    subject number. The subject name is the collection name followed
    by the subject number, e.g. C{Breast004}.
    
    @param collection: the AIRC collection name
    @param source: the input parent directory
    @param pattern: the subject directory name match pattern
        (default L{airc.AIRCCollection.subject_pattern})
    @return: the subject name => directory dictionary
    """
    
    airc_coll = airc.collection_with_name(collection)
    pat = pattern or airc_coll.subject_pattern
    sbj_dir_dict = {}
    with xnat_helper.connection() as xnat:
        for d in os.listdir(source):
            match = re.match(pat, d)
            if match:
                # The XNAT subject name.
                sbj_lbl = SUBJECT_FMT % (collection, int(match.group(1)))
                # The subject source directory.
                sbj_dir_dict[sbj_lbl] = os.path.join(source, d)
                logger.debug("Discovered QIN pipeline test subject subdirectory: %s" % d)
    
    return sbj_dir_dict

def delete_subjects(*subject_names):
    """
    Deletes the given XNAT subjects, if they exist.
    
    @param subject_names: the XNAT subject names
    """
    
    with xnat_helper.connection() as xnat:
        for sbj_lbl in subject_names:
            sbj = xnat.get_subject('QIN', sbj_lbl)
            if sbj.exists():
                sbj.delete()
                logger.debug("Deleted the XNAT test subject %s." % sbj_lbl)
