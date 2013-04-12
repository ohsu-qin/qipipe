import sys, os, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import staging
from test.unit.pipelines.pipelines_helper import get_xnat_subjects, clear_xnat_subjects

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'breast')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'staging')
"""The test results directory."""

COLLECTION = "Breast"
"""The QIN test collection."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False, keep_inputs=True))
config.update_config(cfg)

class TestStagingWorkflow:
    """Registration pipeline unit tests."""
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_staging(self):
        """
        Run the registration pipeline and verify that the registered images are created
        in XNAT.
        """
        
        logger.debug("Testing the registration pipeline on %s..." % FIXTURE)

        # The test subject => directory dictionary.
        sbj_dir_dict = get_xnat_subjects(COLLECTION, FIXTURE)
        # Delete any existing test subjects.
        clear_xnat_subjects(sbj_dir_dict.iterkeys())
        
        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'data')
        work = os.path.join(RESULTS, 'work')

        # Run the workflow.
        logger.debug("Executing the registration workflow...")
        sessions = staging.run(COLLECTION, dest=dest, base_dir=work, *sbj_dir_dict.itervalues())

        # Verify the result.
        for sbj in sbj_dir_dict.iterkeys():
            assert_true(sbj.exists(), "Subject not created in XNAT: %s" % sbj.label())
        for sess in sessions:
            assert_true(sess.exists(), "Session not created in XNAT: %s" % sess)
        
        # Cleanup.
        for sbj in sbj_dir_dict.iterkeys():
            sbj.delete(delete_files=True)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
