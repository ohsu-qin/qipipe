"""
TCIA CTP preparation utilities.
"""

import os
from ..helpers.dicom_tags import read_tags

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
        for root, subdirs, files in os.walk(d):
            for f in files:
                # Read the Patient ID tag
                path = os.path.join(root, f)
                ds = read_tags(path)
                pnt_id = ds.PatientID
                # Escape colon, equal and space.
                pnt_id = "\\".join(pnt_id.split(' '))
                pnt_id = "\\".join(pnt_id.split(':'))
                pnt_id = "\\".join(pnt_id.split('='))
                # The escaped source id maps to the TCIA target id. 
                id_map[pnt_id] = prefix + str(pnt_nbr)
    return id_map
