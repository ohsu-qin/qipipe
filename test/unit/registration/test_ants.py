from nose.tools import *
import os, glob, shutil

import logging
logger = logging.getLogger(__name__)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.registration import ants

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'registration', 'breast', 'Breast03', 'Session01')
"""The test fixture."""

RESULT = os.path.join(ROOT, 'results', 'registration', 'ants')
"""The test results."""

WORK = os.path.join(RESULT, 'work')
"""The work directory."""

OUTPUT = os.path.join(RESULT, 'registered')
"""The registered output images directory."""

class TestANTS:
    """ANTS registration unit tests."""
    
    def setup(self):
        shutil.rmtree(RESULT, True)
    
    def teardown(self):
        shutil.rmtree(RESULT, True)
    
    def test_registration(self):
        images = []
        for root, _, _ in os.walk(FIXTURE):
            images.extend(glob.glob(root + '/*.nii.gz'))
        rdict = ants.register(output=OUTPUT, work=WORK, *images)
        # Verify that each input is registered.
        for f in images:
            registered = f.replace('.nii.gz', 'Registered.nii.gz')
            assert_equal(registered, rdict[f], "Missing registration mapping: %s" % registered)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
