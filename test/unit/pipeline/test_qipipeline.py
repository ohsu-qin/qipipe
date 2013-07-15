import os, sys, shutil, distutils
from nose.tools import *

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.project import project
from qipipe.pipeline import qipipeline as qip
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

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'qipipeline')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestQIPipeline:
    """
    QIN Pipeline unit tests.
    
    Note:: a precondition for running this test is that the environment variable
    ``QIN_DATA`` is set to the AIRC ``HUANG_LAB`` mount point, e.g.::
    
        QIN_DATA=/Volumes/HUANG_LAB
    
    Note:: the modeling workflow is only executed if the ``fastfit`` executable is
    found.
    """
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
    
    def test_breast(self):
        data = os.getenv('QIN_DATA')
        if data:
            fixture = os.path.join(RESULT, 'data', 'breast', 'BreastChemo3')
            os.makedirs(fixture)
            src = os.path.join(data, 'Breast_Chemo_Study', 'BreastChemo3', 'Visit1')
            dest = os.path.join(fixture, 'Visit1')
            os.symlink(src, dest)
            self._test_collection('Breast', fixture)
        else:
            logger.info("Skipping the QIN pipeline unit Breast test, since the QIN_DATA environment variable is not set.")
    
    def test_sarcoma(self):
        data = os.getenv('QIN_DATA')
        if data:
            fixture = os.path.join(RESULT, 'data', 'sarcoma', 'Subj_1')
            os.makedirs(fixture)
            src = os.path.join(data, 'Sarcoma', 'Subj_1', 'Visit1')
            dest = os.path.join(fixture, 'Visit1')
            os.symlink(src, dest)
            self._test_collection('Sarcoma', fixture)
        else:
            logger.info("Skipping the QIN pipeline unit Sarcoma test, since the QIN_DATA environment variable is not set.")
    
    def _test_collection(self, collection, fixture):
        """
        Run the pipeline on the given collection and verify that scans are created in XNAT.
        
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
        
        # Check whether the modeling workflow is executable.
        if not distutils.spawn.find_executable('fastfit'):
            opts['modeling'] = False

        # The test subject => directory dictionary.
        sbj_dir_dict = get_subjects(collection, fixture)
        # The test subjects.
        subjects = sbj_dir_dict.keys()
        # The test source directories.
        sources = sbj_dir_dict.values()
        
        with xnat_helper.connection() as xnat:
            # Delete any existing test subjects.
            delete_subjects(project(), *subjects)

            # Run the staging and registration workflows, but not the modeling.
            logger.debug("Executing the QIN pipeline...")
            reg_specs = qip.run(collection, *sources, dest=dest, work=work, **opts)

            # Verify the result.
            for sbj, sess, recon in reg_specs:
                reg_obj = xnat.get_reconstruction(project(), sbj, sess, recon)
                assert_true(reg_obj.exists(), "The %s %s reconstruction %s was not created in XNAT" % (sbj, sess, recon))
            
            # Delete the test subjects.
            delete_subjects(project(), *subjects)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
