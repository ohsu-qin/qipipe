import os
import re
from qipipe.staging import airc_collection as airc
from qipipe.staging.iterator import SUBJECT_FMT
from .logging import logger

def subject_sources(collection, source):
    """
    Infers the XNAT subject names from the given source directory.
    The source directory contains subject subdirectories. The
    subdirectories are matched against the
    :attr:`qipipe.staging.airc_collection.AIRCCollection.subject_pattern`
    for the given collection.

    :param collection: the AIRC collection name
    :param source: the input parent directory
    :param pattern: the subject directory name match pattern (default
        is the :class:`qipipe.staging.airc_collection.AIRCCollection`
        *subject_pattern*)
    :return: the subject {*name*: *directory*} dictionary
    """
    logger(__name__).debug("Detecting %s subjects from %s..." %
          (collection, source))
    airc_coll = airc.collection_with_name(collection)
    pat = airc_coll.subject_pattern
    sbj_dir_dict = {}
    for d in os.listdir(source):
        match = re.match(pat, d)
        if match:
            # The XNAT subject name.
            subject = SUBJECT_FMT % (collection, int(match.group(1)))
            # The subject source directory.
            sbj_dir_dict[subject] = os.path.join(source, d)
            logger(__name__).debug(
                "Discovered XNAT test subject %s subdirectory: %s" %
                (subject, d))

    return sbj_dir_dict