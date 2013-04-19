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
    
    xnat = xnat_helper.facade()
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
    @return: the XNAT subject => directory dictionary
    """
    
    xnat = xnat_helper.facade()
    airc_coll = airc.collection_with_name(collection)
    pat = pattern or airc_coll.subject_pattern
    sbj_dir_dict = {}
    for d in os.listdir(source):
        match = re.match(pat, d)
        if match:
            # The XNAT subject label.
            sbj_nm = SUBJECT_FMT % (collection, int(match.group(1)))
            logger.debug("Checking whether the test subject %s exists in XNAT..." % sbj_nm)
            # Get the XNAT subject.
            sbj = xnat.interface.select('/project/QIN/subject/' + sbj_nm)
            # If the subject does not exist, then set the label.
            if not sbj.exists():
                sbj.label = sbj_nm
            sbj_dir_dict[sbj] = os.path.join(source, d)
            logger.debug("Discovered QIN pipeline test subject subdirectory: %s" % d)
    
    return sbj_dir_dict

def clear_xnat_subjects(*subjects):
    """
    Deletes the given XNAT subjects, if they exist.
    
    @param subjects: the XNAT subjects
    """
    
    for sbj in subjects:
        if sbj.exists():
            label = sbj.label()
            sbj.delete(delete_files=True)
            logger.debug("Deleted the QIN pipeline test subject from XNAT: %s" % label)
