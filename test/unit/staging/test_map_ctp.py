from nose.tools import *
import os, glob, shutil

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.staging import CTPPatientIdMap

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'fix_dicom', 'sarcoma')

class TestMapCTP:
    """CTP id map unit tests."""
    
    def test_id_map(self):
        expected = {'111710': 'QIN-SARCOMA-01-0003'}
        id_map = CTPPatientIdMap()
        id_map.map_dicom_files('Sarcoma03', FIXTURE)
        assert_equal(expected, id_map, "CTP id map incorrect: %s" % id_map)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
