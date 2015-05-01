import os
from nose.tools import (assert_true, assert_is_not_none)
from nipype.interfaces.dcmstack import MergeNifti
from qipipe.pipeline import (roi, qipipeline)
from ... import (ROOT, PROJECT)
from ...helpers.logging import logger
from ...unit.pipeline.staged_test_base import StagedTestBase

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixtures directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'roi')
"""The test results directory."""


class TestROIWorkflow(StagedTestBase):
    """
    ROI workflow unit tests.
    
    This test exercises the ROI workflow on the Breast and Sarcoma test
    fixture images.
    """

    def __init__(self):
        super(TestROIWorkflow, self).__init__(logger(__name__), FIXTURES,
                                               RESULTS)

    def tearDown(self):
        super(TestROIWorkflow, self).tearDown()

    def test_breast(self):
        self._test_breast()
    
    def test_sarcoma(self):
        self._test_sarcoma()

    def _run_workflow(self, subject, session, scan, *images, **opts):
        """
        Executes :meth:`qipipe.pipeline.roi.run` on the given input.
        
        :param subject: the input subject
        :param session: the input session
        :param scan: the input scan number
        :param images: the scan images
        :param opts: the :class:`qipipe.pipeline.modeling.ROIWorkflow`
            initializer options
        :return: the :meth:`qipipe.pipeline.roi.run` result
        """
        # Make the 4D time series from the test fixture inputs.
        merge = MergeNifti(in_files=list(images),
                           out_format=qipipeline.SCAN_TS_RSC)
        time_series = merge.run().outputs.out_file
        logger(__name__).debug("Testing the ROI workflow on the %s %s time"
                               " series %s..." %
                               (subject, session, time_series))
        
        return ROI.run(PROJECT, subject, session, scan, time_series, **opts)

    def _verify_result(self, xnat, subject, session, scan, result):
        # Verify that the ROI XNAT resource was created.
        rsc_obj = xnat.find(PROJECT, subject, session, scan=scan, resource=result)
        assert_is_not_none(rsc_obj, "The %s %s scan %d XNAT ROI resource object was"
                                    " not created" % (subject, session, scan))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
