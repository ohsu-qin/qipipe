import os
import re
import glob
import shutil
from nose.tools import (assert_true, assert_is_not_none)
import nipype.pipeline.engine as pe
from nipype.interfaces.dcmstack import MergeNifti
from qipipe.pipeline import (mask, qipipeline)
from ... import (project, ROOT)
from ...helpers.logging import logger
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
    
    This test exercises the mask workflow on three volumes of one visit in each
    of the Breast and Sarcoma studies.
    
    :Note: This test is disabled pending a time series test fixture.
     
     TODO - Make a time series test fixture and enable the tests.
    """

    def __init__(self):
        super(TestMaskWorkflow, self).__init__(logger(__name__), FIXTURES,
                                               RESULTS)

    def tearDown(self):
        super(TestMaskWorkflow, self).tearDown()

    def test_breast(self):
        self._test_breast()
    
    def test_sarcoma(self):
        self._test_sarcoma()

    def _run_workflow(self, subject, session, *images, **opts):
        """
        Executes :meth:`qipipe.pipeline.mask.run` on the given input.
        
        :param subject: the input subject
        :param session: the input session
        :param time_series: the input time series
        :param opts: the :class:`qipipe.pipeline.modeling.MaskWorkflow`
            initializer options
        :return: the :meth:`qipipe.pipeline.mask.run` result
        """
        # Make the 4D time series from the test fixture inputs.
        merge = MergeNifti(in_files=list(images),
                           out_format=qipipeline.SCAN_TS_RSC)
        time_series = merge.run().outputs.out_file
        logger(__name__).debug("Testing the mask workflow on the %s %s time"
                               " series %s..." %
                               (subject, session, time_series))
        
        return mask.run(subject, session, time_series, cfg_file=MASK_CONF,
                        **opts)

    def _verify_result(self, xnat, subject, session, result):
        # Verify that the mask XNAT resource was created.
        rsc_obj = xnat.find(project(), subject, session, resource=result)
        assert_is_not_none(rsc_obj, "The %s %s XNAT mask resource object was"
                                    " not created" % (subject, session))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
