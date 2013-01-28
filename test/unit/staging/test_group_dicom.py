from nose.tools import *
import sys, os, glob, shutil
from qipipe.staging import group_dicom_files

import logging
logger = logging.getLogger(__name__)

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'group_dicom')
# The test results.
RESULTS = os.path.join(ROOT, 'results', 'staging', 'group_dicom')
# The test configuration.
LOG = os.path.join(RESULTS, 'log', 'qipipe.log')
# The test result target.
TARGET = os.path.join(RESULTS, 'data')
# The test result delta.
DELTA = os.path.join(RESULTS, 'delta')

class TestGroupDicom:
    """group_dicom unit tests."""
    
    def test_group_dicom_files(self):
        logger.debug('Testing group_dicom_files...')
        shutil.rmtree(RESULTS, True)
        src_pt_dirs = glob.glob(FIXTURE + '/*')
        opts = dict(target=TARGET, include='*concat*/*')
        group_dicom_files(*src_pt_dirs, **opts)
        # Verify that there are no broken links.
        for root, dirs, files in os.walk(TARGET):
            for f in files:
                tgt_file = os.path.join(root, f)
                assert_true(os.path.islink(tgt_file), "Missing source -> target target link: %s" % tgt_file)
                assert_true(os.path.exists(tgt_file), "Broken source -> target link: %s" % tgt_file)
        # Test incremental delta.
        tgt = os.path.join(TARGET, 'patient01', 'visit02')
        # Clean the partial result.
        shutil.rmtree(tgt, True)
        # Clean the delta tree.
        shutil.rmtree(DELTA, True)
        # Rerun to capture the delta.
        group_dicom_files(*src_pt_dirs, delta=DELTA, **opts)
        delta = os.path.join(DELTA, 'patient01', 'visit02')
        assert_true(os.path.islink(delta), "Missing delta -> target link: %s" % delta)
        assert_true(os.path.exists(delta), "Broken delta -> target link: %s" % delta)
        real_tgt = os.path.realpath(tgt)
        real_delta = os.path.realpath(delta)
        assert_equal(real_tgt, real_delta, "Delta does not reference the target:  %s" % delta)
        non_delta = os.path.join(DELTA, 'patient01', 'visit01')
        assert_false(os.path.exists(non_delta), "Incorrectly added a target -> delta link in %s" % non_delta)
        # Cleanup.
        shutil.rmtree(RESULTS, True)
        logger.debug('group_dicom_files test completed.')


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
