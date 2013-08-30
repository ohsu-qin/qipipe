import os, glob, shutil
from nose.tools import (assert_equal, assert_not_equal)
from qipipe.helpers.logging_helper import logger
from qipipe.interfaces import GroupDicom
from test import ROOT

# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'breast')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'staging', 'breast')

class TestGroupDicom:
    """GroupDicom interface unit tests."""
    
    def test_link_dicom(self):
        logger(__name__).debug("Testing the GroupDicom interface on %s..." % FIXTURE)
        shutil.rmtree(RESULTS, True)
        sbj_dirs = glob.glob(FIXTURE + '/*')
        for d in sbj_dirs:
            grouper = GroupDicom(collection='Breast', subject_dir=d, session_pat='Visit*', dicom_pat='*concat*/*', dest=RESULTS)
            result = grouper.run()
            ser_dirs = result.outputs.series_dirs
            assert_not_equal(0, len(ser_dirs), "GroupDicom did not create the output series directories in %s" % RESULTS)
            assert_equal(ser_dirs, result.outputs.series_dirs, "The GroupDicom output is incorrect: %s" % ser_dirs)
        # Cleanup.
        shutil.rmtree(RESULTS, True)
        logger(__name__).debug("GroupDicom interface test completed")

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
