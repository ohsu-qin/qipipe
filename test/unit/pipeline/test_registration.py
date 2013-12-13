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

    def test_breast(self):
        super(TestRegistrationWorkflow, self)._test_breast(technique='mock')
    
    def test_sarcoma(self):
        super(TestRegistrationWorkflow, self)._test_sarcoma(technique='mock')

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
        # The target location.
        dest = os.path.join(RESULTS, subject, session)
        # Execute the workflow.
        return registration.run(subject, session, images,
                                bolus_arrival_index=bolus_arv_ndx, mask=mask,
                                cfg_file=REG_CONF, resource=RESOURCE, dest=dest,
                                **opts)

    def _verify_result(self, xnat, subject, session, result):
        # Confirm that the XNAT resource object was created.
        rsc_obj = xnat.find(project(), subject, session, resource=RESOURCE)
        assert_is_not_none(rsc_obj,
                           "The %s %s %s XNAT registration resource object was"
                           " not created" % (subject, session, RESOURCE))
        out_split = [os.path.split() for f in result]
        out_dirs, out_files = zip(out_split)
        rsc_files = rsc_obj.files().get()
        assert_equals(out_files, rsc_files,
                      "The %s %s %s XNAT registration resource files are incorrect"
                      ": %s" % (subject, session, RESOURCE, rsc_files))
        
        # Confirm that the output files were created.
        dest = os.path.join(RESULTS, subject, session)
        dest_files = [os.path.join(dest, f) for f in os.listdir(dest)]
        assert_equals(set(dest_files), set(result),
                      "The %s %s %s XNAT registration result is incorrect:"
                      " %s" % (subject, session, RESOURCE, result))



if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
