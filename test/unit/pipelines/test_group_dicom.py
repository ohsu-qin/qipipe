from nose.tools import *
import os, re, glob, shutil

import logging
logger = logging.getLogger(__name__)

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'group_dicom')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'staging', 'group_dicom')

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines.group_dicom import GroupDicom


class TestGroupDicom:
    """GroupDicom interface unit tests."""
    
    def test_group_dicom(self):
        logger.debug("Testing the GroupDicom interface on %s..." % FIXTURE)
        shutil.rmtree(RESULTS, True)
        sbj_dirs = glob.glob(os.path.join(FIXTURE, '*'))
        grp = GroupDicom(subject_dirs=sbj_dirs, visit_pat='Visit*', dicom_pat='*concat*/*' dest=RESULTS)
        result = grp.run()
        ser_dirs = result.outputs.series_dirs
        assert_equal([], ser_dirs, "The GroupDicom output is incorrect: " % ser_dirs)
        # Cleanup.
        shutil.rmtree(RESULTS, True)
        logger.debug("GroupDicom interface test completed")

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
