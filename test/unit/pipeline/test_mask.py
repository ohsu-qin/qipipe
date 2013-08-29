import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe
from qipipe.helpers.logging_helper import logger
from qipipe.pipeline import mask
from test.helpers.project import project
from test.unit.pipeline.staged_test_base import (StagedTestBase, ROOT)

FIXTURES = os.path.join(ROOT, 'fixtures', 'registration')
"""The test fixtures directory."""

MASK_CONF = os.path.join(ROOT, 'conf', 'mask.cfg')
"""The test mask configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'mask')
"""The test results directory."""


class TestMaskWorkflow(StagedTestBase):
    """
    Mask workflow unit tests.
    
    This test exercises the mask workflow on three series of one visit in each of the
    Breast and Sarcoma studies.
    """
    
    def __init__(self):
        super(TestMaskWorkflow, self).__init__(logger(__name__), FIXTURES, RESULTS)
    
    def test_breast(self):
        self._test_breast()
    
    def test_sarcoma(self):
        self._test_sarcoma()
    
    def _run_workflow(self, fixture, input_dict, base_dir=None):
        """
        Executes :meth:`qipipe.pipeline.mask.run` on the given input.
        
        :param fixture: the test fixture directory
        :param input_dict: the input *{subject: {session: [images]}}* to mask
        :param base_dir: the workflow exection directory
        :return: the :meth:`qipipe.pipeline.mask.run` result
        """
        logger(__name__).debug("Testing the mask workflow on %s..." % fixture)
        return mask.run(input_dict, cfg_file=MASK_CONF, base_dir=base_dir)
    
    def _verify_result(self, xnat, input_dict, recon):
        for sbj, sess_dict in input_dict.iteritems():
            for sess in sess_dict:
                recon_obj = xnat.find(project(), sbj, sess,
                    reconstruction=recon)
                assert_is_not_none(recon_obj, "The %s %s %s XNAT reconstruction"
                    " object was not created" % (sbj, sess, mask.MASK_RECON))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
