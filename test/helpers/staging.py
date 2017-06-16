import os
import re
from qipipe import staging
from qipipe.helpers.constants import SUBJECT_FMT
from .logging import logger


def subject_sources(collection, source):
    """
    Infers the XNAT subject names from the given source directory.
    The *source* argument directory contains the subject
    directories. The directories are matched against
    the :attr:`qipipe.staging.image_collection.Collection.patterns`
    ``subject`` regular expression for the given collection.

    :param collection: the image collection name
    :param source: the input parent directory
    :return: the subject {*name*: *directory*} dictionary
    """
    _logger = logger(__name__)
    _logger.debug("Detecting %s subjects from %s..." % (collection, source))
    img_coll = staging.image_collection.with_name(collection)
    pat = img_coll.patterns.subject
    sbj_dir_dict = {}
    for d in os.listdir(source):
        match = re.match(pat, d)
        if match:
            # The XNAT subject name.
            subject = SUBJECT_FMT % (collection, int(match.group(1)))
            # The subject source directory.
            sbj_dir_dict[subject] = os.path.join(source, d)
            _logger.debug("Discovered XNAT test subject %s"
                          " subdirectory: %s" % (subject, d))

    return sbj_dir_dict
