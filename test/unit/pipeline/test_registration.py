import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.project import project
from qipipe.pipeline import registration
from test.unit.pipeline.staged_test_base import (StagedTestBase, ROOT)

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


class TestRegistrationWorkflow(StagedTestBase):
    """
    Registration workflow unit tests.
    
    This test exercises the registration workflow on three series of one visit in each of the
    Breast and Sarcoma studies.
    """
    
    def __init__(self):
        super(TestRegistrationWorkflow, self).__init__(logger, FIXTURES, RESULTS, use_mask=True)
    
    def test_breast_with_ants(self):
        self._test_breast()
    
    def test_sarcoma_with_ants(self):
        self._test_sarcoma()
    
    def test_breast_with_fnirt(self):
        self._test_breast(technique='fnirt')
    
    def _run_workflow(self, fixture, input_dict, base_dir=None):
        """
        Executes :meth:`qipipe.pipeline.registration.run` on the given input.
        
        :param fixture: the test fixture directory
        :param input_dict: the input {subject: {session: ([images], mask)}} to register
        :param base_dir: the workflow exection directory
        :return: the :meth:`qipipe.pipeline.registration.run` result
        """
        logger.debug("Testing the registration workflow on %s..." % fixture)
        # Add in the mask.
        return registration.run(input_dict, cfg_file=REG_CONF, base_dir=base_dir)
    
    def _verify_result(self, xnat, input_dict, recon):
        for sbj, sess_dict in input_dict.iteritems():
            for sess in sess_dict:
                recon_obj = xnat.find(project(), sbj, sess,
                    reconstruction=recon)
                assert_is_not_none(recon_obj, "The %s %s %s XNAT reconstruction"
                    " object was not created" % (sbj, sess, recon))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
