import unittest
import os, glob, shutil
from qipipe.staging import staging

# The test fixture.
FIXTURE = os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'staging')

RESULTS = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'staging')

LINKS = os.path.join(RESULTS, 'data')

DELTA = os.path.join(RESULTS, 'delta')

class TestStaging(unittest.TestCase):
    """TCIA staging unit tests."""
    
    def test_link_dicom_files(self):
        shutil.rmtree(RESULTS, True)
        dirs = glob.glob(FIXTURE + '/*/*')
        opts = {'delta': DELTA, 'target': LINKS, 'include': '*concat*/*'}
        dirs.append(opts)
        args = dirs
        staging.link_dicom_files(*args)
        for i in range(1, 2):
            # Verify the source -> target link.
            d = os.path.join(LINKS, 'patient01', "visit%02d" % i)
            self.assertTrue(os.path.exists(d), "Missing source -> target DICOM file link in %s" % d)
            # Verify the target -> delta link.
            d = os.path.join(DELTA, 'patient01', "visit%02d" % i)
            self.assertTrue(os.path.exists(d), "Missing target -> delta link in %s" % d)
        # Test incremental delta.
        visit2 = os.path.join(LINKS, 'patient01', 'visit02')
        shutil.rmtree(visit2, True)
        shutil.rmtree(DELTA, True)
        staging.link_dicom_files(*args)
        self.assertTrue(os.path.exists(visit2), "Missing source -> target DICOM file link in %s" % visit2)
        delta2 = os.path.join(DELTA, 'patient01', 'visit02')
        self.assertTrue(os.path.exists(delta2), "Missing target -> delta link in %s" % visit2)
        delta1 = os.path.join(DELTA, 'patient01', 'visit01')
        self.assertFalse(os.path.exists(delta1), "Incorrectly added a target -> delta link in %s" % delta1)
        # Cleanup.
        shutil.rmtree(RESULTS, True)

if __name__ == "__main__":
    unittest.main()
