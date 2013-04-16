import sys, os, re, shutil
from nose.tools import *

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.interfaces import MapCTP
from test.unit.staging.test_map_ctp import (COLLECTION, SUBJECTS, PAT)

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'map_ctp')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestMapCTP:
    """Map CTP unit tests."""
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
    
    def test_map_ctp(self):
        logger.debug("Testing Map CTP on %s..." % SUBJECTS)
        map_ctp = MapCTP(collection=COLLECTION, patient_ids=SUBJECTS, dest=RESULTS)
        result = map_ctp.run()
        prop_file = result.outputs.out_file
        assert_true(os.path.exists(prop_file), "Property file was not created: %s" % prop_file)
        assert_equal(RESULTS, os.path.dirname(prop_file), "Property file was not created in %s: %s" % (RESULTS, prop_file))
        for line in open(prop_file).readlines():
            qin_id, ctp_suffix = re.match(PAT, line).groups()
            assert_true(qin_id in SUBJECTS, "Subject id not found: %s" % qin_id)
            qin_nbr = int(qin_id[-2:])
            ctp_nbr = int(ctp_suffix)
            assert_equal(qin_nbr, ctp_nbr, "Patient number incorrect; expected: %d found: %d" % (qin_nbr, ctp_nbr))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
