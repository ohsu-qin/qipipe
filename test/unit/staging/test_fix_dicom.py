import os
import glob
import shutil
from nose.tools import assert_equal
from qipipe.staging.fix_dicom import fix_dicom_headers
from qidicom import reader
from ... import ROOT
from ...helpers.logging import logger

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

    def setUp(self):
        shutil.rmtree(RESULTS, True)
        os.makedirs(RESULTS)

    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_breast(self):
        fixed = fix_dicom_headers(COLLECTION, SUBJECT, FIXTURE, dest=RESULTS)
        # Verify the result.
        for ds in reader.iter_dicom(*fixed):
            assert_equal(ds.BodyPartExamined, 'CHEST',
                         "Incorrect Body Part: %s" % ds.BodyPartExamined)
            assert_equal(ds.PatientID, SUBJECT, "Incorrect Patient ID: %s" %
                                                ds.PatientID)

    def test_sarcoma(self):
        fixed = fix_dicom_headers(COLLECTION, SUBJECT, FIXTURE, dest=RESULTS)
        # Verify the result.
        for ds in reader.iter_dicom(*fixed):
            assert_equal(ds.BodyPartExamined, 'CHEST',
                         "Incorrect Body Part: %s" % ds.BodyPartExamined)
            assert_equal(ds.PatientID, SUBJECT,
                         "Incorrect Patient ID: %s" % ds.PatientID)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
