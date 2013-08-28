from nose.tools import *
import os, glob
from qipipe.staging.staging_helper import *

from qipipe.helpers.logging_helper import logger


# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'sarcoma', 'Subj_1')

class TestStagingHelper:
    """staging_helper unit tests."""
    
    def test_group_dicom_files_by_series(self):
        dicom_files = glob.glob(FIXTURE + '/V*/*concat*/*')
        groups = group_dicom_files_by_series(*dicom_files)
        assert_equal(set([9, 10]), set(groups.keys()), "The DICOM series grouping is incorrect: %s" % groups)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
