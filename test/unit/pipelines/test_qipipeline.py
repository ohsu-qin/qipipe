import os, sys, shutil, distutils
from nose.tools import *

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.helpers.xnat_helper import PROJECT
from qipipe.pipelines import qipipeline as qip
from qipipe.helpers.dicom_helper import iter_dicom
from qipipe.helpers import xnat_helper
from qipipe.staging import airc_collection as airc
from qipipe.helpers.xnat_helper import delete_subjects
from qipipe.staging.staging_helper import get_subjects
from qipipe.helpers.ast_config import read_config

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

REG_CONF = os.path.join(ROOT, 'conf', 'registration.cfg')
"""The test registration configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'qipipeline')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestQIPipeline:
    """
    QIN Pipeline unit tests.
    
    Note:: a precondition for running this test is that the following environment
    variables are set:

    - ``QIN_BREAST_INPUT``: the input AIRC test Breast fixture parent directory to test

    - ``QIN_SARCOMA_INPUT``: the input AIRC test Sarcoma fixture parent directory to test
    The test directories must conform to the subject/visit/dicom directory patterns
    defined in :meth:`airc`.
    
    Note:: the PK mapping workflow is only executed if the ``fastfit`` executable is
    found.
    
    The recommended test input is three series for one visit from each collection.
    The pipeline is run serially, and takes app. two hours per visit on this input.
    """
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
    
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
        
        :param collection: the AIRC collection name
        :param fixture: the test input
        """
        logger.debug("Testing the QIN pipeline on %s..." % fixture)

        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'data')
        work = os.path.join(RESULTS, 'work')
        
        # The test registration configuration.
        reg_opts = dict(read_config(REG_CONF))
        
        # The pipeline options.
        opts = dict(registration=reg_opts)
        
        # Check whether the PK mapping workflow is executable.
        if not distutils.spawn.find_executable('fastfit'):
            opts['pk_mapping'] = False

        # The test subject => directory dictionary.
        sbj_dir_dict = get_subjects(collection, fixture)
        # The test subjects.
        subjects = sbj_dir_dict.keys()
        # The test source directories.
        sources = sbj_dir_dict.values()
        
        with xnat_helper.connection() as xnat:
            # Delete any existing test subjects.
            delete_subjects(PROJECT, *subjects)

            # Run the staging and registration workflows, but not the PK mapping.
            logger.debug("Executing the QIN pipeline...")
            reg_specs = qip.run(collection, *sources, dest=dest, work=work, **opts)

            # Verify the result.
            for sbj, sess, recon in reg_specs:
                reg_obj = xnat.get_reconstruction(PROJECT, sbj, sess, recon)
                assert_true(reg_obj.exists(), "The %s %s reconstruction %s was not created in XNAT" % (sbj, sess, recon))
            
            # Delete the test subjects.
            delete_subjects(PROJECT, *subjects)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
