import os
import re
import glob
import shutil
from nose.tools import (assert_equal, assert_is_not_none)
import nipype.pipeline.engine as pe
from qipipe.pipeline import registration
from test import (project, ROOT)
from test.helpers.logging_helper import logger
from test.helpers.xnat_test_helper import generate_unique_name
from test.unit.pipeline.staged_test_base import StagedTestBase

REG_CONF = os.path.join(ROOT, 'conf', 'registration.cfg')
"""The test registration configuration."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'registration')
"""The test fixtures directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'registration')
"""The test results directory."""

RESOURCE = generate_unique_name(__name__)
"""The XNAT registration resource name."""


class TestRegistrationWorkflow(StagedTestBase):

    """
    Registration workflow unit tests.

    This test exercises the registration workflow on three series of one visit
    in each of the Breast and Sarcoma studies.
    """

    def __init__(self):
        super(TestRegistrationWorkflow, self).__init__(
            logger(__name__), FIXTURES, RESULTS, use_mask=True)

    # def test_breast_with_ants(self):
    #     self._test_breast()
    # 
    # def test_sarcoma_with_ants(self):
    #     self._test_sarcoma()

    def test_breast_with_fnirt(self):
        self._test_breast(technique='fnirt')

    def _run_workflow(self, fixture, subject, session, images, mask, **opts):
        """
        Executes :meth:`qipipe.pipeline.registration.run` on the given input.

        :param fixture: the test fixture directory
        :param subject: the input subject
        :param session: the input session
        :param images: the input image files
        :param mask: the input mask file
        :param opts: additional workflow options
        :return: the :meth:`qipipe.pipeline.registration.run` result
        """
        self._logger.debug("Testing the registration workflow on %s..." %
                               fixture)
        # A reasonable bolus uptake index.
        bolus_arv_ndx = min(3, len(images) / 3)
        return registration.run(subject, session, images,
                                bolus_arrival_index=bolus_arv_ndx, mask=mask,
                                cfg_file=REG_CONF, resource=RESOURCE, **opts)

    def _verify_result(self, xnat, subject, session, result):
        rsc_obj = xnat.find(project(), sbj, sess, resource=RESOURCE)
        assert_is_not_none(rsc_obj, "The %s %s %s XNAT registration resource"
                           " object was not created" % (sbj, sess, result))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
