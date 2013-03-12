from nose.tools import *
import os, glob, shutil

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.staging import create_ctp_id_map

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'edit_dicom', 'breast', 'subject03')

class TestCTP:
    """CTP unit tests."""
    
    def test_id_map(self):
        expected = {'111710': 'QIN-BREAST-02-0003'}
        actual = create_ctp_id_map('QIN-BREAST-02', FIXTURE)
        assert_equal(expected, actual, "CTP id map incorrect: %s" % actual)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
