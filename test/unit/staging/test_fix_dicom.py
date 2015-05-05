import os
import glob
import shutil
from nose.tools import assert_equal

from qipipe.staging.fix_dicom import fix_dicom_headers
from qidicom import reader
from ... import ROOT

# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'sarcoma', 'Subj_1')

# The test results.
RESULTS = os.path.join(ROOT, 'results', 'staging', 'fix_dicom')

# The collection name.
COLLECTION = 'Sarcoma'

# The new subject.
SUBJECT = 'Sarcoma003'

class TestFixDicom(object):
    """Fix DICOM header unit tests."""

    def test_fix_dicom_headers(self):
        shutil.rmtree(RESULTS, True)
        dest = os.path.dirname(RESULTS)
        fixed = fix_dicom_headers(COLLECTION, SUBJECT, FIXTURE, dest=dest)
        # Verify the result.
        for ds in reader.iter_dicom(*fixed):
            assert_equal(ds.BodyPartExamined, 'CHEST',
                         "Incorrect Body Part: %s" % ds.BodyPartExamined)
            assert_equal(
                ds.PatientID, SUBJECT, "Incorrect Patient ID: %s" % ds.PatientID)
        # Cleanup.
        shutil.rmtree(RESULTS, True)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
