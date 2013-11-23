import os
import re
import shutil
from nose.tools import (assert_equal, assert_true)
from qipipe.interfaces import MapCTP
from test import ROOT
from test.helpers.logging_helper import logger
from test.unit.staging.test_map_ctp import (COLLECTION, SUBJECTS, PAT)

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'map_ctp')
"""The test results directory."""


class TestMapCTP(object):

    """Map CTP unit tests."""

    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_map_ctp(self):
        logger(__name__).debug("Testing Map CTP on %s..." % SUBJECTS)
        map_ctp = MapCTP(
            collection=COLLECTION, patient_ids=SUBJECTS, dest=RESULTS)
        result = map_ctp.run()
        prop_file = result.outputs.out_file
        assert_true(os.path.exists(prop_file), "Property file was not created:"
                    " %s" % prop_file)
        assert_equal(os.path.dirname(prop_file), RESULTS, "Property file was"
                     " not created in %s: %s" % (RESULTS, prop_file))
        for line in open(prop_file).readlines():
            qin_id, ctp_suffix = re.match(PAT, line).groups()
            assert_true(
                qin_id in SUBJECTS, "Subject id not found: %s" % qin_id)
            qin_nbr = int(qin_id[-2:])
            ctp_nbr = int(ctp_suffix)
            assert_equal(ctp_nbr, qin_nbr, "Patient number incorrect; expected:"
                         " %d found: %d" % (qin_nbr, ctp_nbr))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
