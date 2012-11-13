"""
TCIA CTP preparation utilities.
"""

import os
from ..helpers import read_dicom_tags

def id_map(prefix, dirs):
    """
    Returns the CTP map for the DICOM files in the given directories. The map is a
    dictionary
    
    @param prefix: the target CTP Patient ID collection prefix, e.g. `QIN-BREAST-02-`
    @param dirs: the source patient DICOM directories
    @return: the source => target map
    @rtype: dict
    """
    id_map = dict()
    for d in dirs:
        # The patient number is extracted from the directory name.
        pnt_match = pat.search(os.path.basename(d))
        if not pnt_match:
            continue
        pnt_nbr = int(pnt_match.group(0))
        for pnt_id in read_dicom_tags(['Patient ID'], d):
            # Escape colon, equal and space.
            pnt_id = "\\".join(pnt_id.split(' '))
            pnt_id = "\\".join(pnt_id.split(':'))
            pnt_id = "\\".join(pnt_id.split('='))
            # The escaped source id maps to the TCIA target id. 
            id_map[pnt_id] = prefix + str(pnt_nbr)
    return id_map
