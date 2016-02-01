import os
from glob import glob
from nose.tools import (assert_true, assert_is_not_none)
from nipype.interfaces.dcmstack import MergeNifti
from qiutil.which import which
import qixnat
from qipipe.staging.iterator import iter_stage
from qipipe.pipeline import (roi, qipipeline)
from ... import (ROOT, PROJECT, CONF_DIR)
from ...helpers.logging import logger
from .volume_test_base import VolumeTestBase

STAGING_FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test staging fixtures directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'roi')
"""The test results directory."""


class TestROIWorkflow(VolumeTestBase):
    """
    ROI workflow unit tests.
    
    This test exercises the ROI workflow on the Breast and Sarcoma test
    fixture images.
    """

    def __init__(self):
        super(TestROIWorkflow, self).__init__(logger(__name__), RESULTS)

    def test_breast(self):
        if not which('bolero_mask_conv'):
            logger(__name__).debug('Skipping ROI test since bolero_mask_conv'
                                   ' is unavailable.')
            return
        fixture = os.path.join(STAGING_FIXTURES, 'breast', 'BreastChemo3')
        # The test input session directories.
        sess_dirs = glob(fixture + '/*')
        # The test scan inputs.
        inputs = list(iter_stage(PROJECT, 'Breast', *sess_dirs))
        for args in self.stage('Breast'):
            self._test_workflow(inputs, *args)

    def test_sarcoma(self):
        if not which('bolero_mask_conv'):
            return
        # FIXME - Breast works, but Sarcoma fails with error:
        #     File "/Users/loneyf/bin/bolero_mask_conv", line 60, in main
        #       mask_slice[y, x] = 1
        #     IndexError: index 187 is out of bounds for axis 0 with size 24
        # TODO - Get a combo of volumes and ROIs that work, if not too
        #   unwieldly, otherwise delete the Sarcoma fixture and this test
        #   method.

    def _test_workflow(self, scan_inputs, project, subject, session, scan,
                       *images):
        """
        Executes :meth:`qipipe.pipeline.roi.run` on the input scans.
        
        :param scan_inputs: the :meth:`qipipe.staging.iter_stage` iterator
        :param project: the input project name
        :param subject: the input subject name
        :param session: the input session name
        :param scan: the input scan number
        :param images: the input volume images
        """
        # Only test if there is ROI input.
        scan_iter = (scan_input for scan_input in scan_inputs
                     if scan_input.subject == subject and
                        scan_input.session == session and
                        scan_input.scan == scan)
        scan_input = next(scan_iter, None)
        assert_is_not_none(scan_input, "%s %s Scan %d not found in staging:" %
                                       (subject, session, scan))
        roi_inputs = scan_input.iterators.roi
        if not roi_inputs:
            logger(__name__).debug("Skipping testing the ROI workflow on %s %s"
                                   " Scan %d, since it has no ROI input files."
                                   (subject, session, scan))
            return
        # Make the 4D time series from the test fixture inputs.
        # TODO - newer nipype has out_path MergeNifti input field. Set
        # that field to out_path=RESULTS below. Work-around is to move
        # the file to RESULTS below.
        merge = MergeNifti(in_files=list(images),
                           out_format=qipipeline.SCAN_TS_RSC)
        time_series = merge.run().outputs.out_file
        # Work-around for nipype bug described above.
        _, ts_fname = os.path.split(time_series)
        ts_dest = os.path.join(RESULTS, ts_fname)
        import shutil
        shutil.move(time_series, ts_dest)
        time_series = ts_dest
        # End of work-around.
        logger(__name__).debug("Testing the ROI workflow on the %s %s time"
                               " series %s..." %
                               (subject, session, time_series))
        with qixnat.connect() as xnat:
            xnat.delete(project, subject)
            result = roi.run(project, subject, session, scan, time_series, 
                             *roi_inputs, base_dir=self.base_dir,
                              config_dir=CONF_DIR)
            assert_is_not_none(result, "The %s %s Scan %d ROI pipeline did not"
                                       " run" % (subject, session, scan))
            # Find the ROI resource.
            rsc = xnat.find_one(project, subject, session, scan=scan,
                                resource=result)
            try:
                assert_is_not_none(rsc, "The %s %s Scan %d %s resource was not"
                                        " created" %
                                        (subject, session, scan, result))
            finally:
                xnat.delete(project, subject)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
