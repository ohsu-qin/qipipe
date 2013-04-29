import sys, os, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import staging
from qipipe.helpers import xnat_helper
from test.helpers.xnat_test_helper import get_xnat_subjects, clear_xnat_subjects

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
        # The test subject => directory dictionary.
        self._sbj_dir_dict = get_xnat_subjects(COLLECTION, FIXTURE)
        # Delete any existing test subjects.
        clear_xnat_subjects(*self._sbj_dir_dict.keys())
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
        clear_xnat_subjects(*self._sbj_dir_dict.keys())

    def test_staging(self):
        """
        Run the staging pipeline and verify that the registered images are created
        in XNAT.
        """
        
        logger.debug("Testing the registration pipeline on %s..." % FIXTURE)

        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'data')
        work = os.path.join(RESULTS, 'work')

        # Run the workflow.
        logger.debug("Executing the staging workflow...")
        session_specs = staging.run(COLLECTION, dest=dest, base_dir=work, *self._sbj_dir_dict.itervalues())

        # Verify the result.
        with xnat_helper.connection() as xnat:
            for sbj_lbl in self._sbj_dir_dict.iterkeys():
                sbj = xnat.get_subject('QIN', sbj_lbl)
                assert_true(sbj.exists(), "The subject was not created in XNAT: %s" % sbj.label())
            for sbj_lbl, sess_lbl in session_specs:
                sess = xnat.get_session('QIN', sbj_lbl, sess_lbl)
                assert_true(sess.exists(), "The session not created in XNAT: %s" % sess)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
