"""
TCIA CTP preparation utilities.
"""

import sys, os, re
from .staging_error import StagingError 
from ..helpers.dicom_helper import iter_dicom_headers

import logging
logger = logging.getLogger(__name__)

def create_ctp_id_map(collection, *paths, **opts):
    """
    Returns the CTP map for the DICOM files in the given directories. The map is a
    dictionary augmented with a {format} method that prints the CTP map properties.
    
    @parm collection: the target CTP Patient ID collection name, e.g. C{QIN-BREAST-02}
    @param paths: the source patient DICOM directories
    @param opts: the CTPPatientIdMap options
    @return: the source => target map
    @rtype: CTPPatientIdMap
    """
    return CTPPatientIdMap(collection, *paths, **opts)
    
    
class CTPPatientIdMap(dict):
    # The ID lookup entry format.
    _FMT = "ptid/%(dicom id)s=%(ctp id)s"

    def __init__(self, collection, *paths, **opts):
        """
        Builds the {DICOM: CTP} patient id map for the DICOM files in the given directories.
        
        The following options are supported:
            - first_only: flag indicating whether to only read the first image Patient ID tag (default False)
    
        @param collection: the target CTP Patient ID collection name, e.g. C{QIN-BREAST-02}
        @param paths: the source patient DICOM directories
        @param opts: the keyword options
        """

        ctp_fmt = collection + "-%04d"
        # The RE to extract the patient number suffix.
        pat = re.compile('\d+$')
        first_only = opts.has_key('first_only') and opts['first_only']
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
                dcm_id = ds.PatientID
                # Escape colon, equal and space.
                dcm_id = "\\".join(dcm_id.split(' '))
                dcm_id = "\\".join(dcm_id.split(':'))
                dcm_id = "\\".join(dcm_id.split('='))
                # The escaped source id maps to the TCIA target id. 
                if self.has_key(dcm_id):
                    if self[dcm_id] != ctp_id:
                        tmpl = "Inconstent mapping: %(dcm)s -> %(ctp)s vs %(dcm)s -> %(bad)s in series %(s)d instance %(i)d"
                        raise StagingError(tmpl %  dict(dcm=self[dcm_id], ctp=ctp_id, bad=dcm_id, s=ds.SeriesNumber, i=ds.InstanceNumber))
                else:
                    tmpl = "Mapped %(dcm)s -> %(ctp)s from  series %(s)d instance %(i)d."
                    logger.debug(tmpl % dict(dcm=dcm_id, ctp=ctp_id, s=ds.SeriesNumber, i=ds.InstanceNumber))
                    self[dcm_id] = ctp_id
                # If all of this patient's Patient ID tags are the same, then skip the remaining files.
                if first_only:
                    logger.debug("Skipping the remaining files in %s..." % d)
                    break
        
    def write(self, dest=sys.stdout):
        """
        Writes this id map in the standard CTP format.
        
        @param dest: the IO stream on which to write this map (default stdout)
        """
        for dcm_id in sorted(self.iterkeys()):
            # Escape colon and blank in the source patient id.
            esc_id = re.sub(r'([: =])', r'\\\1', dcm_id)
            print >> dest, CTPPatientIdMap._FMT % {'dicom id': esc_id, 'ctp id': self[dcm_id]}
