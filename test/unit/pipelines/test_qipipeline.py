import os, sys, shutil
from nose.tools import *

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import qipipeline as qip
from qipipe.helpers.dicom_helper import iter_dicom
from qipipe.helpers import xnat_helper
from qipipe.staging import airc_collection as airc
from test.helpers.xnat_test_helper import get_subjects, delete_subjects
from test.helpers.registration import ANTS_REG_TEST_OPTS

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'qipipeline')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestQIPipeline:
    """
    QIN Pipeline unit tests.
    
    @attention: a precondition for running this test is that the following environment
    variables are set:
        - C{QIN_BREAST_INPUT}: the input AIRC test Breast subject directory to test
        - C{QIN_SARCOMA_INPUT}: the input AIRC test Sarcoma subject directory to test
    
    The recommended test input is three series for one visit from each collection.
    The pipeline is run serially, and takes app. two hours per visit on this input.
    """
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        pass #shutil.rmtree(RESULTS, True)
    
    def test_breast(self):
        fixture = os.getenv('QIN_BREAST_INPUT')
        if fixture:
            self._test_collection('Breast', fixture)
        else:
            logger.info("Skipping the QIN pipeline unit Breast test, since the QIN_BREAST_INPUT environment variable is not set.")
    
    def test_sarcoma(self):
        fixture = os.getenv('QIN_SARCOMA_INPUT')
        if fixture:
            self._test_collection('Sarcoma', fixture)
        else:
            logger.info("Skipping the QIN pipeline unit Sarcoma test, since the QIN_SARCOMA_INPUT environment variable is not set.")
    
    def _test_collection(self, collection, fixture):
        """
        Run the pipeline on the given collection and verify the following:
            - scans are created in XNAT
        
        @param collection: the AIRC collection name
        @param fixture: the test input
        """
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

            # Run the staging and registration workflows, but not the PK mapping.
            logger.debug("Executing the QIN pipeline...")
            reg_specs = qip.run(collection, *sources, dest=dest, work=work,
                register=ANTS_REG_TEST_OPTS, pk_mapping=False)

            # Verify the result.
            for sbj, sess, recon in reg_specs:
                reg_obj = xnat.get_reconstruction('QIN', sbj, sess, recon)
                assert_true(reg_obj.exists(), "The %s %s reconstruction %s was not created in XNAT" % (sbj, sess, recon))
            
            # Delete the test subjects.
            #delete_subjects(*subjects)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
