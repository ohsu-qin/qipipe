"""
TCIA CTP preparation utilities.
"""

import sys
import os
import re
from .ctp_config import ctp_collection_for_name
from qiutil.logging import logger
from .staging_error import StagingError

# OHSU - this is OHSU-specific.
# TODO - make the format a config item.
PROP_FMT = 'QIN-%s-OHSU.ID-LOOKUP.properties'
"""
The format for the Patient ID map file name specified by CTP.
"""


def map_ctp(collection, *subjects, **opts):
    """
    Creates the TCIA patient id map. The map is written to a
    property file in the destination directory. The property file
    name is given by :meth:`property_filename`.
    
    :param collection: the image collection
    :param subjects: the subject names
    :param opts: the following keyword option:
    :keyword dest: the destination directory
    :return: the subject map file path
    """
    # Make the CTP id map.
    ctp_map = CTPPatientIdMap()
    ctp_map.add_subjects(collection, *subjects)
    # Write the id map property file.
    dest = opts.get('dest')
    if dest:
        dest = os.path.abspath(dest)
        if not os.path.exists(dest):
            os.makedirs(dest)
    else:
        dest = os.getcwd()
    logger(__name__).debug("Creating the TCIA subject map in %s..." % dest)
    out_file = os.path.join(dest, property_filename(collection))
    output = open(out_file, 'w')
    ctp_map.write(output)
    output.close()
    logger(__name__).debug("Created the TCIA subject map %s." % out_file)
    
    return out_file


def property_filename(collection):
    """
    Returns the CTP id map property file name for the given collection.
    The Sarcoma collection is capitalized in the file name, Breast is not.
    """
    if collection == 'Sarcoma':
        return PROP_FMT % collection.upper()
    else:
        return PROP_FMT % collection


class CTPPatientIdMap(dict):
    """
    CTPPatientIdMap is a dictionary augmented with a :meth:`map_subjects`
    input method to build the map and a :meth:`write` output method to
    print the CTP map properties.
    """
    
    SOURCE_PAT = re.compile("""
        ([a-zA-Z]+)     # The study name
        _?              # An optional underscore delimiter
        (\d+)$          # The patient number
    """, re.VERBOSE)
    """
    The input Patient ID pattern is the study name followed by a number,
    e.g. ``Breast10``.
    """
    
    CTP_FMT = '%s-%04d'
    """
    The CTP Patient ID format with arguments (CTP collection name, input
    Patient ID number).
    """
    
    MAP_FMT = 'ptid/%s=%s'
    """
    The ID lookup entry format with arguments (input Paitent ID, CTP
    patient id).
    """
    
    MSG_FMT = 'Mapped the QIN patient id %s to the CTP subject id %s.'
    """
    The log message format with arguments (input Paitent ID, CTP patient id).
    """
    
    def add_subjects(self, collection, *patient_ids):
        """
        Adds the input => CTP Patient ID association for the given input
        DICOM patient ids.
        
        :param collection: the image collection name
        :param patient_ids: the DICOM Patient IDs to map
        :raise StagingError: if an input patient id format is not the study
            followed by the patient number
        """
        ctp_coll = ctp_collection_for_name(collection)
        for in_pt_id in patient_ids:
            match = CTPPatientIdMap.SOURCE_PAT.match(in_pt_id)
            if not match:
                raise StagingError("Unsupported input QIN patient id format:"
                                   " %s" % in_pt_id)
            pt_nbr = int(match.group(2))
            ctp_id = CTPPatientIdMap.CTP_FMT % (ctp_coll, pt_nbr)
            self[in_pt_id] = ctp_id
            logger(__name__).debug(
                CTPPatientIdMap.MSG_FMT % (in_pt_id, ctp_id))
    
    def write(self, dest=sys.stdout):
        """
        Writes this id map in the standard CTP format.
        
        :param dest: the IO stream on which to write this map (default stdout)
        """
        for qin_id in sorted(self.iterkeys()):
            print >> dest, CTPPatientIdMap.MAP_FMT % (qin_id, self[qin_id])
