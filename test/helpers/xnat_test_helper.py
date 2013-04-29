import os, re
from base64 import b64encode as encode
from qipipe.staging import airc_collection as airc
from qipipe.staging.staging_helper import SUBJECT_FMT
from qipipe.helpers import xnat_helper

import logging
logger = logging.getLogger(__name__)

def generate_subject_label(name):
    """
    Makes a subject label that is unique to the given test name.
    
    @param name: the test name
    @return: the test subject label
    """
    
    return 'Test_' + encode(name).strip('=')
    
def delete_subjects(*labels):
    """
    Deletes each given test subject, if it exists.
    
    @param labels: the labels of the subjects to delete
    """
    
    with xnat_helper.connection() as xnat:
        for lbl in labels:
            sbj = xnat.interface.select('/project/QIN/subject/' + lbl)
            if sbj.exists():
                sbj.delete()

def get_xnat_subjects(collection, source, pattern=None):
    """
    Infers the XNAT subject labels from the given source directory.
    The source directory contains subject subdirectories.
    The match pattern matches on the subdirectories and captures the
    subject number. The subject label is the collection name followed
    by the subject number, e.g. C{Breast004}.
    
    @param collection: the AIRC collection name
    @param source: the input parent directory
    @param pattern: the subject directory name match pattern
        (default L{airc.AIRCCollection.subject_pattern})
    @return: the subject label => directory dictionary
    """
    
    airc_coll = airc.collection_with_name(collection)
    pat = pattern or airc_coll.subject_pattern
    sbj_dir_dict = {}
    with xnat_helper.connection() as xnat:
        for d in os.listdir(source):
            match = re.match(pat, d)
            if match:
                # The XNAT subject label.
                sbj_lbl = SUBJECT_FMT % (collection, int(match.group(1)))
                # The subject source directory.
                sbj_dir_dict[sbj_lbl] = os.path.join(source, d)
                logger.debug("Discovered QIN pipeline test subject subdirectory: %s" % d)
    
    return sbj_dir_dict

def clear_xnat_subjects(*subject_labels):
    """
    Deletes the given XNAT subjects, if they exist.
    
    @param subject_labels: the XNAT subject labels
    """
    
    with xnat_helper.connection() as xnat:
        for sbj_lbl in subject_labels:
            sbj = xnat.get_subject('QIN', sbj_lbl)
            if sbj.exists():
                sbj.delete(delete_files=True)
                logger.debug("Deleted the QIN pipeline test subject from XNAT: %s" % sbj_lbl)
