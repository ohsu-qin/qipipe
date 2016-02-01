"""
This module updates the qiprofile database scan information
from a XNAT experiment.
"""

class ScanError(Exception):
    pass


def update(session, xscan):
    """
    Updates the scan content for the given qiprofile session
    database object from the given XNAT scan object.
    
    :param session: the target qiprofile Session to update
    :param xscan: the XNAT scan object
    """
    pass
