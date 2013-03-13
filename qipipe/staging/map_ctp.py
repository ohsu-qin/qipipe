"""
TCIA CTP preparation utilities.
"""

import sys, os, re
from .ctp_config import ctp_study
from ..helpers.dicom_helper import iter_dicom_headers

import logging
logger = logging.getLogger(__name__)
    
class CTPPatientIdMap(dict):
    """
    CTPPatientIdMap is a dictionary augmented with a L{map_dicom_files} input method
    to build the map and a L{write} output method to print the CTP map properties.
    """
    
    # The input subject id pattern.
    INPUT_PAT = "([^\d]+)(\d+)$"
    
    # The CTP subject id format.
    CTP_FMT = "%(study)s-%(sbj_nbr)04d"
    
    # The ID lookup entry format.
    MAP_FMT = "ptid/%(dicom_id)s=%(ctp_id)s"

    def map_dicom_files(self, subject_id, *dicom_files):
        """
        Builds the {DICOM: CTP} subject id map for the given DICOM files.
        The subject id must be a AIRC collection followed by a digit, e.g.
        C{Breast02}.
    
        @param subject_id: the subject id
        @param dicom_files: the DICOM files or directories to map
        """
        collection, suffix = re.match(CTPPatientIdMap.INPUT_PAT, subject_id).groups()
        sbj_nbr = int(suffix)
        study = ctp_study(collection)
        ctp_id = CTPPatientIdMap.CTP_FMT % dict(study=study, sbj_nbr=sbj_nbr)
        for ds in iter_dicom_headers(*dicom_files):
            dcm_id = ds.PatientID
            # The escaped source id maps to the TCIA target id. 
            if dcm_id not in self:
                self[dcm_id] = ctp_id
                tmpl = "Mapped the DCM patient id %s to the CTP subject id %s."
                logger.debug(tmpl % (dcm_id, subject_id))
        
    def write(self, dest=sys.stdout):
        """
        Writes this id map in the standard CTP format.
        
        @param dest: the IO stream on which to write this map (default stdout)
        """
        for dcm_id in sorted(self.iterkeys()):
            # Escape colon and blank in the source subject id.
            esc_id = re.sub(r'([: =])', r'\\\1', dcm_id)
            print >> dest, CTPPatientIdMap.MAP_FMT % dict(dicom_id=esc_id, ctp_id=self[dcm_id])
