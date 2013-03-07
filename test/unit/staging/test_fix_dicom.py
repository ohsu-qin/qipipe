from nose.tools import *
import os, glob, shutil

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.staging import fix_dicom_headers
from qipipe.helpers.dicom_helper import iter_dicom

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'fix_dicom', 'sarcoma', 'subject03')
# The test results.
RESULTS = os.path.join(ROOT, 'results', 'staging', 'fix_dicom', 'sarcoma', 'subject03')
# The collection name.
COLLECTION = 'Sarcoma'

class TestFixDicom:
    """Fix DICOM header unit tests."""
    
    def test_fix_dicom_headers(self):
        shutil.rmtree(RESULTS, True)
        fix_dicom_headers(FIXTURE, RESULTS, COLLECTION)
        # Verify the result.
        for ds in iter_dicom(RESULTS):
            assert_equal('CHEST', ds.BodyPartExamined, "Incorrect Body Part: %s" % ds.BodyPartExamined)
            assert_equal('Sarcoma03', ds.PatientID, "Incorrect Patient ID: %s" % ds.PatientID)
        # Cleanup.
        shutil.rmtree(RESULTS, True)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
