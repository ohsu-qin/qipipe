import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.project import project
from qipipe.pipeline import registration
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from test.unit.pipeline.xnat_scan_test_base import (XNATScanTestBase, ROOT)

REG_CONF = os.path.join(ROOT, 'conf', 'registration.cfg')
"""The test registration configuration."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'registration')
"""The test fixtures directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'registration')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestRegistrationWorkflow(XNATScanTestBase):
    """
    Registration workflow unit tests.
    
    This test exercises the registration workflow on three series of one visit in each of the
    Breast and Sarcoma studies.
    """
    
    def __init__(self):
        super(TestRegistrationWorkflow, self).__init__(logger, FIXTURES, RESULTS)
    
    def test_breast_with_ants(self):
        self._test_breast()
    
    def test_sarcoma_with_ants(self):
        self._test_sarcoma()
    
    def test_breast_with_fnirt(self):
        self._test_breast(technique='fnirt')
        
    def _run_workflow(self, fixture, *inputs, **opts):
        """
        Executes :meth:`qipipe.pipeline.registration.run` on the input sessions.
        
        :param fixture: the test fixture directory
        :param inputs: the (subject, session) tuples
        :param opts: the following keyword options:
        :keyword base_dir: the workflow exection directory
        :return: the :meth:`qipipe.pipeline.modeling.run` result
        """
        logger.debug("Testing the registration workflow on %s..." % fixture)
        return registration.run(*inputs, cfg_file=REG_CONF, **opts)
    
    def _verify_result(self, xnat, inputs, result):
        # The return value is the reconstruction name.
        recon = result
        for sbj, sess in inputs:
            recon_obj = xnat.get_reconstruction(project(), sbj, sess, recon)
            assert_true(recon_obj.exists(),
                "The %s %s %s XNAT reconstruction object was not created" % (sbj, sess, recon))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
