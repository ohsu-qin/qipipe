import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import shutil
from qipipe.helpers.logging import logger
from nose.tools import *
from qipipe.registration import ants

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'registration', 'breast', 'patient03', 'visit01')
# The test results.
RESULT = os.path.join(ROOT, 'results', 'registration', 'ants')
WORK = os.path.join(RESULT, 'work')
OUTPUT = os.path.join(RESULT, 'registered')

class TestANTS:
    """ANTS registration unit tests."""
    
    def test_registration(self):
        shutil.rmtree(RESULT, True)
        rdict = ants.register(FIXTURE, output=OUTPUT, work=WORK)
        # Verify that each input is registered.
        for fn in os.listdir(FIXTURE):
            f = os.path.join(FIXTURE, fn)
            rfn = fn.replace('.dcm', 'Registered.nii.gz')
            assert_equal(rfn, rdict[fn], "Missing registration mapping: %s" % rfn)
            assert_true(rfn in rout, "Missing registration file: %s" % rfn)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
