import os, sys, shutil
from nose.tools import *

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import qipipeline as qip
from qipipe.pipelines import QIPipeline
from qipipe.helpers.dicom_helper import iter_dicom
from qipipe.helpers import xnat_helper
from qipipe.staging import airc_collection as airc
from test.helpers.xnat_test_helper import get_subjects, delete_subjects

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixtures directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'qipipeline')
"""The test results directory."""

COLLECTION = "Breast"
"""The QIN test collection."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestPipeline:
    """Pipeline unit tests."""
    
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
        Run the pipeline on the given collection and verify the following:
            - scans are created in XNAT
        
        @param collection: the AIRC collection name
        @attention: This test does not verify the CTP staging area nor that the
            image files are correctly uploaded. These features should be
            verified manually.
        """
        fixture = os.path.join(FIXTURES, collection.lower())
        logger.debug("Testing the QIN pipeline on %s..." % fixture)

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
            delete_subjects(*subjects)

            # Run the workflow.
            logger.debug("Executing the staging workflow...")
            recon_specs = qip.run(collection, *sources, dest=dest, work=work)

            # Verify the result.
            for sbj, sess, recon in recon_specs:
                recon_obj = xnat.get_reconstruction('QIN', sbj, sess, recon)
                assert_true(recon_obj.exists(), "The %s %s reconstruction %s was not created in XNAT" % (sbj, sess, recon))
        
            # Delete the test subjects.
            delete_subjects(*subjects)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
