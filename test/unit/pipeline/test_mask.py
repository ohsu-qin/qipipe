import os
from nose.tools import (assert_true, assert_is_not_none)
from nipype.interfaces.dcmstack import MergeNifti
import qixnat
from qipipe.pipeline import mask
from qipipe.helpers.constants import SCAN_TS_RSC
from ... import (ROOT, CONF_DIR)
from ...helpers.logging import logger
from .volume_test_base import VolumeTestBase

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'mask')
"""The test results directory."""


class TestMaskWorkflow(VolumeTestBase):
    """
    Mask workflow unit tests.
    
    This test exercises the mask workflow on three volumes of one visit in each
    of the Breast and Sarcoma studies.
    """
    
    def __init__(self):
        super(TestMaskWorkflow, self).__init__(logger(__name__), RESULTS)
    
    def test_breast(self):
        for args in self.stage('Breast'):
            self._test_workflow(*args)
    
    def test_sarcoma(self):
        for args in self.stage('Sarcoma'):
            self._test_workflow(*args)
    
    def _test_workflow(self, project, subject, session, scan, *images):
        """
        Executes :meth:`qipipe.pipeline.mask.run` on the input scans.
        
        :param xnat: the XNAT facade instance
        :param project: the input project name
        :param subject: the input subject name
        :param session: the input session name
        :param scan: the input scan number
        :param images: the input 3D NIfTI images to model
        """
        # Make the 4D time series from the test fixture inputs.
        merge = MergeNifti(in_files=list(images),
                           out_format=SCAN_TS_RSC)
        time_series = merge.run().outputs.out_file
        logger(__name__).debug("Testing the mask workflow on the %s %s"
                               " Scan %d time series %s..." %
                               (subject, session, scan, time_series))
        # Run the workflow.
        with qixnat.connect() as xnat:
            xnat.delete(project, subject)
            result = mask.run(project, subject, session, scan, time_series,
                              base_dir=self.base_dir, config_dir=CONF_DIR)
            # Find the mask resource.
            rsc = xnat.find_one(project, subject, session, scan=scan,
                                resource=result)
            try:
                assert_is_not_none(rsc, "The %s %s scan %d %s resource was not"
                                        " created" %
                                        (subject, session, scan, result))
            finally:
                xnat.delete(project, subject)

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
