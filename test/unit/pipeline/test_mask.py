import os
import re
import glob
import shutil
from nose.tools import (assert_equal, assert_is_not_none)
import nipype.pipeline.engine as pe
from qipipe.pipeline import mask
from ... import (project, ROOT)
from ...helpers.logging_helper import logger
from ...unit.pipeline.staged_test_base import StagedTestBase

FIXTURES = os.path.join(ROOT, 'fixtures', 'registration')
"""The test fixtures directory."""

MASK_CONF = os.path.join(ROOT, 'conf', 'mask.cfg')
"""The test mask configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'mask')
"""The test results directory."""


class TestMaskWorkflow(StagedTestBase):
    """
    Mask workflow unit tests.
    
    This test exercises the mask workflow on three series of one visit in each
    of the Breast and Sarcoma studies.
    
    :Note: This test is disabled pending a time series test fixture.
     
     TODO - Make a time series test fixture and enable the tests.
    """

    def __init__(self):
        super(TestMaskWorkflow, self).__init__(
            logger(__name__), FIXTURES, RESULTS)

    # def test_breast(self):
    #     self._test_breast()
    # 
    # def test_sarcoma(self):
    #     self._test_sarcoma()

    def _run_workflow(self, fixture, subject, session, **opts):
        """
        Executes :meth:`qipipe.pipeline.mask.run` on the given input.
        
        :param fixture: the test fixture directory
        :param subject: the input subject
        :param session: the input session
        :param opts: the target workflow options
        :return: the :meth:`qipipe.pipeline.mask.run` result
        """
        logger(__name__).debug("Testing the mask workflow on %s..." % fixture)
        input_dict = {subject: {session: opts}}
        return mask.run(*inputs, cfg_file=MASK_CONF, **opts)

    def _verify_result(self, xnat, subject, session, result):
        rsc_obj = xnat.find(project(), subject, session, resource=result)
        assert_is_not_none(rsc_obj, "The %s %s XNAT mask resource object"
                           " was not created" % (subject, session))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
