from nose.tools import *
import sys, os, glob, shutil

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'breast')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'staging', 'breast')

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.helpers.logging_helper import logger
from qipipe.interfaces import GroupDicom

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
