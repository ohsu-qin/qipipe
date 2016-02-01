"""
This module updates the qiprofile database modeling information
from a XNAT experiment.
"""

class ModelingError(Exception):
    pass


def update(session, resource):
    """
    Updates the modeling content for the given qiprofile session
    database object from the given XNAT modeling resource object.
    
    :param session: the target qiprofile Session to update
    :param resource: the XNAT modeling resource object
    """
    # TODO
    pass
