import os
import re
from qipipe.staging import collection
from qipipe.helpers.constants import SUBJECT_FMT
from .logging import logger


def subject_sources(collection, source):
    """
    Infers the XNAT subject names from the given source directory.
    The *source* argument directory contains the subject
    directories. The directories are matched against
    the :attr:`qipipe.staging.collection.Collection.patterns`
    :attr:`qipipe.staging.collection.Patterns.subject` regular
    expression for the given collection.

    :param collection: the image collection name
    :param source: the input parent directory
    :return: the subject {*name*: *directory*} dictionary
    """
    logger(__name__).debug("Detecting %s subjects from %s..." %
          (collection, source))
    coll = collection.with_name(collection)
    pat = coll.patterns.subject
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
