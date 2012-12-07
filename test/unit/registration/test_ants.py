import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import shutil
import logging
from nose.tools import *
from qipipe.registration import ants

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'registration', 'breast', 'patient03', 'visit01')
# The test results.
RESULT = os.path.join(ROOT, 'results', 'registration', 'ants', 'patient03', 'visit01')

class TestANTS:
    """ANTS registration unit tests."""
    
    def test_registration(self):
        shutil.rmtree(RESULT, True)
        ants.register(FIXTURE, RESULT)
        # Verify that each input is registered.
        results = set(os.listdir(RESULT))
        for f in os.listdir(FIXTURE):
            registered = f.replace('.nii', 'Registered.nii')
            assert_true(registered in results, "Missing registration result: %s" % f)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
