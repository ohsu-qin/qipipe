from nose.tools import *
import os, glob, shutil

import logging
logger = logging.getLogger(__name__)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.staging.map_ctp import CTPPatientIdMap

COLLECTION = 'Sarcoma'
"""The test collection."""

SUBJECTS = ["Sarcoma%02d" %  i for i in range(8, 12)]

PAT = "ptid/(Sarcoma\d{2})\s*=\s*QIN-\w+-\d{2}-(\d{4})"

class TestMapCTP:
    """Map CTP unit tests."""
    
    def test_map_ctp(self):
        logger.debug("Testing Map CTP on %s..." % SUBJECTS)
        ctp_map = CTPPatientIdMap()
        ctp_map.add_subjects(COLLECTION, *SUBJECTS)
        for sbj in SUBJECTS:
            ctp_id = ctp_map.get(sbj)
            assert_is_not_none(ctp_id, "Subject was not mapped: %s" % sbj)
            qin_nbr = int(sbj[-2:])
            ctp_nbr = int(ctp_id[-4:])
            assert_equal(qin_nbr, ctp_nbr, "Patient number incorrect; expected: %d found: %d" % (qin_nbr, ctp_nbr))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
