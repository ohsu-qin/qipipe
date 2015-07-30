import os
import re
import glob
import shutil
from nose.tools import assert_is_not_none
import nipype.pipeline.engine as pe
from nipype.interfaces.dcmstack import MergeNifti
import qixnat
# Modeling requires the proprietary OHSU fastfit module.
# If fastfit is not found, then the tests are skipped.
try:
    import fastfit
except ImportError:
    fastfit = None
from qipipe.pipeline import modeling
from qipipe.pipeline import qipipeline
from ... import (ROOT, PROJECT)
from ...helpers.logging import logger
from ...unit.pipeline.volume_test_base import VolumeTestBase

MODELING_CONF = os.path.join(ROOT, 'conf', 'modeling.cfg')
"""The test registration configuration."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'staged')

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'modeling')
"""The test results directory."""


class TestModelingWorkflow(VolumeTestBase):
    """
    Modeling workflow unit tests.
    This test exercises the modeling workflow on the QIN Breast and Sarcoma
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
        super(TestModelingWorkflow, self).__init__(
            logger(__name__), FIXTURES, RESULTS)

    def test_breast(self):
        if fastfit:
            for args in self.stage('Breast'):
                self._test_workflow(*args)
        else:
            logger(__name__).debug('Skipping modeling test since fastfit'
                                   ' is unavailable.')

    def test_sarcoma(self):
        if fastfit:
            for args in self.stage('Sarcoma'):
                self._test_workflow(*args)

    def _test_workflow(self, project, subject, session, scan, *images):
        """
        Executes :meth:`qipipe.pipeline.modeling.run` on the input scans.
        
        :param project: the input project name
        :param subject: the input subject name
        :param session: the input session name
        :param scan: the input scan number
        :param images: the input 3D NiFTI images to model
        """
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
        logger(__name__).debug("Testing the modeling workflow on the %s %s"
                               " time series %s..." %
                               (subject, session, time_series))
        with qixnat.connect() as xnat:
            xnat.delete(project, subject)
            result = modeling.run(project, subject, session, scan, time_series, 
                                  config=MODELING_CONF, technique='mock',
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
