from nose.tools import *
import os, glob, shutil

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.helpers.dicom_helper import edit_dicom_headers
from qipipe.helpers.dicom_helper import iter_dicom
from dicom import datadict as dd

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'helpers', 'edit_dicom')
# The test results.
RESULT = os.path.join(ROOT, 'results', 'helpers', 'edit_dicom')

class TestEditDicom:
    """DICOM edit unit tests."""
    
    def test_edit_dicom_files(self):
        # Clear the result before starting.
        shutil.rmtree(RESULT, True)
        # The tag name => value map.
        tnv = dict(PatientID='Test Patient', BodyPartExamined='BREAST', PixelData='')
        # The tag => value map.
        tv = {dd.tag_for_name(name): value for name, value in tnv.iteritems()}
        shutil.rmtree(RESULT, True)
        files = set(edit_dicom_headers(FIXTURE, RESULT, **tnv))
        for ds in iter_dicom(RESULT):
            assert_true(ds.filename in files)
            for t, v in tv.iteritems():
                assert_equal(v, ds[t].value)
        # Cleanup.
        shutil.rmtree(RESULT, True)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
