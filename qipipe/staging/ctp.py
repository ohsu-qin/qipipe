"""
TCIA CTP preparation utilities.
"""

import sys
import os
import re
from qipipe.helpers.logging import logger
from ..helpers.dicom_helper import iter_dicom_headers

def create_ctp_id_map(collection, *paths):
    """
    Returns the CTP map for the DICOM files in the given directories. The map is a
    dictionary augmented with a {format} method that prints the CTP map properties.
    
    @param collection: the target CTP Patient ID collection name, e.g. C{QIN-BREAST-02}
    @param paths: the source patient DICOM directories
    @return: the source => target map
    @rtype: CTPPatientIdMap
    """
    return CTPPatientIdMap(collection, *paths)

class CTPPatientIdMap(dict):
    # The ID lookup entry format.
    _FMT = "ptid/%(dicom id)s=%(ctp id)s"

    def __init__(self, collection, *paths):
        """
        Builds the {DICOM: CTP} patient id map for the DICOM files in the given directories.
        
        @param collection: the target CTP Patient ID collection name, e.g. C{QIN-BREAST-02}
        @param paths: the source patient DICOM directories
        """

        ctp_fmt = collection + "-%04d"
        # The RE to extract the patient number suffix.
        pat = re.compile('\d+$')
        for d in paths:
            # The patient number is extracted from the directory name.
            pt_match = pat.search(os.path.basename(d))
            if not pt_match:
                logger.warn("Directory name is not recognized as a patient: %s" % d)
                continue
            pt_nbr = int(pt_match.group(0))
            ctp_id = ctp_fmt % pt_nbr
            logger.info("Inferred the CTP patient id %(ctp id)s from the directory name %(dir)s." % {'ctp id': ctp_id, 'dir': d})
            for ds in iter_dicom_headers(d):
                pt_id = ds.PatientID
                # Escape colon, equal and space.
                pt_id = "\\".join(pt_id.split(' '))
                pt_id = "\\".join(pt_id.split(':'))
                pt_id = "\\".join(pt_id.split('='))
                # The escaped source id maps to the TCIA target id. 
                self[pt_id] = ctp_id
        
    def write(self, dest=sys.stdout):
        """
        Writes this id map in the standard CTP format.
        
        @param dest: the IO stream on which to write this map (default stdout)
        """
        for dcm_id in sorted(self.iterkeys()):
            # Escape colon and blank in the source patient id.
            esc_id = re.sub(r'([: =])', r'\\\1', dcm_id)
            print >> dest, CTPPatientIdMap._FMT % {'dicom id': esc_id, 'ctp id': self[dcm_id]}
