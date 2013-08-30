import os
import glob
from nose.tools import assert_equal
from qipipe.staging.staging_helper import *
from qipipe.helpers.logging_helper import logger
from test import ROOT

# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'sarcoma', 'Subj_1')


class TestStagingHelper(object):

    """staging_helper unit tests."""

    def test_group_dicom_files_by_series(self):
        dicom_files = glob.glob(FIXTURE + '/V*/*concat*/*')
        groups = group_dicom_files_by_series(*dicom_files)
        assert_equal(set(groups.keys()), set(
            [9, 10]), "The DICOM series grouping is incorrect: %s" % groups)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
