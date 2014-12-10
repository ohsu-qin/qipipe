import os
import glob
import shutil
from nose.tools import (assert_equal, assert_is_not_none)
from ...helpers.logging import logger
from qipipe.staging.map_ctp import CTPPatientIdMap

COLLECTION = 'Sarcoma'
"""The test collection."""

SUBJECTS = ["Sarcoma%03d" % i for i in range(8, 12)]
"""The test subjects."""

PAT = "ptid/(Sarcoma\d{3})\s*=\s*QIN-\w+-\d{2}-(\d{4})"
"""The CTP map pattern specified by the QIN TCIA curator."""


class TestMapCTP(object):

    """Map CTP unit tests."""

    def test_map_ctp(self):
        logger(__name__).debug("Testing Map CTP on %s..." % SUBJECTS)
        ctp_map = CTPPatientIdMap()
        ctp_map.add_subjects(COLLECTION, *SUBJECTS)
        for sbj in SUBJECTS:
            ctp_id = ctp_map.get(sbj)
            assert_is_not_none(ctp_id, "Subject was not mapped: %s" % sbj)
            qin_nbr = int(sbj[-2:])
            ctp_nbr = int(ctp_id[-4:])
            assert_equal(ctp_nbr, qin_nbr, "Patient number incorrect; expected:"
                         " %d found: %d" % (qin_nbr, ctp_nbr))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
