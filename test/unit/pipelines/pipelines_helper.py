import os, re
from qipipe.staging import airc_collection as airc
from qipipe.staging.staging_helper import SUBJECT_FMT
from qipipe.helpers import xnat_helper

import logging
logger = logging.getLogger(__name__)

def get_xnat_subjects(collection, source):
    """
    @param collection: the AIRC collection name
    @param source: the input parent directory
    @return: the XNAT subject => directory dictionary
    """
    
    xnat = xnat_helper.facade()
    airc_coll = airc.collection_with_name(collection)
    sbj_dir_dict = {}
    for d in os.listdir(source):
        match = re.match(airc_coll.subject_pattern, d)
        if match:
            # The XNAT subject label.
            sbj_nm = SUBJECT_FMT % (collection, int(match.group(1)))
            logger.debug("Checking whether the test subject %s exists in XNAT..." % sbj_nm)
            # Delete the XNAT subject, if necessary.
            sbj = xnat.interface.select('/project/QIN/subject/' + sbj_nm)
            if sbj.exists():
                sbj.delete(delete_files=True)
                logger.debug("Deleted the QIN pipeline test subject from XNAT: %s" % sbj_nm)
            sbj_dir_dict[sbj] = os.path.join(source, d)
            logger.debug("Discovered QIN pipeline test subject subdirectory: %s" % d)
    
    return sbj_dir_dict

def clear_xnat_subjects(subjects):
    """
    Deletes the given XNAT subjects, if they exist.
    
    @param subjects: the XNAT subjects
    """
    
    for sbj in subjects:
        if sbj.exists():
            label = sbj.label()
            sbj.delete(delete_files=True)
            logger.debug("Deleted the QIN pipeline test subject from XNAT: %s" % label)
