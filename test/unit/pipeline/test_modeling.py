import os
import re
import glob
import shutil
from nose.tools import assert_is_not_none
import nipype.pipeline.engine as pe
from nipype.interfaces.dcmstack import MergeNifti
import qixnat
from qipipe.pipeline import modeling
from qipipe.helpers.constants import SCAN_TS_RSC
from ... import (ROOT, PROJECT, CONF_DIR)
from ...helpers.logging import logger
from .volume_test_base import VolumeTestBase

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'modeling')
"""The test results directory."""


class TestModelingWorkflow(VolumeTestBase):
    """
    Modeling workflow unit tests.
    This test exercises the modeling workflow on the Breast and Sarcoma
    study visits in the ``test/fixtures/pipeline/modeling`` test fixture
    directory.
    
    Note:: a precondition for running this test is that the
        ``test/fixtures/pipeline/modeling`` directory contains the series
        stack test data in collection/subject/session format, e.g.::
    
            breast
                Breast003
                    Session01
                        volume009.nii.gz
                        volume023.nii.gz
                         ...
            sarcoma
                Sarcoma001
                    Session01
                        volume011.nii.gz
                        volume013.nii.gz
                         ...
    
        The fixture is not included in the Git source repository due to
        storage constraints.
    
    Note:: this test takes several hours to run on the AIRC cluster.
    """

    def __init__(self):
        super(TestModelingWorkflow, self).__init__(logger(__name__), RESULTS)

    def test_breast(self):
        for args in self.stage('Breast'):
            for technique in modeling.TECHNIQUES:
                self._test_workflow(technique, *args)

    def test_sarcoma(self):
        for args in self.stage('Sarcoma'):
            for technique in modeling.TECHNIQUES:
                self._test_workflow(technique, *args)

    def _test_workflow(self, technique, project, subject, session, scan,
                       *images):
        """
        Executes :meth:`qipipe.pipeline.modeling.run` on the given input.

        :param technique: the built-in modeling technique
        :param project: the input project name
        :param subject: the input subject name
        :param session: the input session name
        :param scan: the input scan number
        :param images: the input 3D NIfTI images to model
        """
        # Make the 4D time series from the test fixture inputs.
        #
        # Note: newer Nipype, e.g. 0.10, has an out_path MergeNifti
        # input field. Until that Nipype version is supported by
        # qipipe, move the file to RESULTS instead.
        #
        # TODO - Add out_path=RESULTS and remove the work-around
        # below when Nipype version >= 0.10 is supported by qipipe.
        merge = MergeNifti(in_files=list(images), out_format=SCAN_TS_RSC)
        time_series = merge.run().outputs.out_file

        # Work around the Nipype bug described above.
        _, ts_fname = os.path.split(time_series)
        ts_dest = os.path.join(RESULTS, ts_fname)
        import shutil
        shutil.move(time_series, ts_dest)
        time_series = ts_dest
        # End of work-around.

        logger(__name__).debug("Testing the %s modeling workflow on the %s %s"
                               " time series %s..." %
                               (technique, subject, session, time_series))
        with qixnat.connect() as xnat:
            xnat.delete(project, subject)
            result = modeling.run(technique, project, subject, session, scan,
                                  SCAN_TS_RSC, time_series, config_dir=CONF_DIR,
                                  base_dir=self.base_dir)
            # Find the modeling resource.
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
