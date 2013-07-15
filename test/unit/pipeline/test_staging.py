import sys, os, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.project import project
from qipipe.pipeline import staging
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from qipipe.staging.staging_helper import get_subjects

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'staging')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False, keep_inputs=True))
config.update_config(cfg)

class TestStagingWorkflow(object):
    """Staging workflow unit tests."""
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
    
    def test_breast(self):
        self._test_collection('Breast')
    
    def test_sarcoma(self):
        self._test_collection('Sarcoma')
    
    def _test_collection(self, collection):
        """
        Run the staging workflow on the given collection and verify that
        the sessions are created in XNAT.

        Note:: This test does not verify the CTP staging area nor that the
        image files are correctly uploaded. These features should be
        verified manually.
        
        :param collection: the AIRC collection name
        """       
        fixture = os.path.join(FIXTURES, collection.lower())
        logger.debug("Testing the staging workflow on %s..." % fixture)

        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'data')
        work = os.path.join(RESULTS, 'work')

        # The test subject => directory dictionary.
        sbj_dir_dict = get_subjects(collection, fixture)
        # The test subjects.
        subjects = sbj_dir_dict.keys()
        # The test source directories.
        sources = sbj_dir_dict.values()
        
        with xnat_helper.connection() as xnat:
            # Delete any existing test subjects.
            delete_subjects(project(), *subjects)

            # Run the pipeline.
            session_specs = staging.run(collection, *sources, base_dir=dest)

            # Verify the result.
            for sbj, sess in session_specs:
                sess_obj = xnat.get_session(project(), sbj, sess)
                assert_true(sess_obj.exists(), "The %s %s session was not created in XNAT" % (sbj, sess))
        
            # Delete the test subjects.
            delete_subjects(project(), *subjects)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
