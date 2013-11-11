import os
import re
import glob
import shutil
from nose.tools import (assert_equal, assert_is_not_none)
import nipype.pipeline.engine as pe
from qipipe.pipeline import reference
from test import project
from test.helpers.logging_helper import logger
from test.unit.pipeline.staged_test_base import (StagedTestBase, ROOT)

FIXTURES = os.path.join(ROOT, 'fixtures', 'registration')
"""The test fixtures directory."""

REF_CONF = os.path.join(ROOT, 'conf', 'reference.cfg')
"""The test reference configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'reference')
"""The test results directory."""


class TestReferenceWorkflow(StagedTestBase):

    """
    Reference workflow unit tests.
    
    This test exercises the reference workflow on three series of one visit in each
    of the Breast and Sarcoma studies.
    """

    def __init__(self):
        super(TestReferenceWorkflow, self).__init__(
            logger(__name__), FIXTURES, RESULTS)

    def test_breast(self):
        self._test_breast()

    def test_sarcoma(self):
        self._test_sarcoma()

    def _run_workflow(self, fixture, input_dict, base_dir=None):
        """
        Executes :meth:`qipipe.pipeline.reference.run` on the given input.
        
        :param fixture: the test fixture directory
        :param input_dict: the input *{subject: {session: [images]}}* to reference
        :param base_dir: the workflow exection directory
        :return: the :meth:`qipipe.pipeline.reference.run` result
        """
        logger(__name__).debug("Testing the reference workflow on %s..." % fixture)
        return reference.run(input_dict, cfg_file=REF_CONF, base_dir=base_dir)

    def _verify_result(self, xnat, input_dict, recon):
        for sbj, sess_dict in input_dict.iteritems():
            for sess in sess_dict:
                recon_obj = xnat.find(project(), sbj, sess,
                                      reconstruction=recon)
                assert_is_not_none(recon_obj, "The %s %s %s XNAT reconstruction"
                                   " object was not created" %
                                   (sbj, sess, reference.REFreg_obj))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
