import os
import re
import glob
import shutil
from nose.tools import (assert_equal, assert_true)
import nipype.pipeline.engine as pe
try:
    from qipipe.pipeline import modeling
except ImportError:
    modeling = None
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
        if modeling:
            for xnat, args, opts in self.stage('Breast'):
                self._test_workflow(xnat, *args, **opts)
        else:
            logger(__name__).debug('Skipping modeling test since fastfit'
                                   ' is unavailable.')

    def test_sarcoma(self):
        if modeling:
            for xnat, args, opts in self.stage('Sarcoma'):
                self._test_workflow(xnat, *args, **opts)

    def _test_workflow(self, xnat, project, subject, session, scan,
                       *images, **opts):
        """
        Executes :meth:`qipipe.pipeline.modeling.run` on the input scans.
        
        :param xnat: the XNAT facade instance
        :param project: the input project name
        :param subject: the input subject
        :param session: the input session
        :param scan: the input scan number
        :param images: the input 3D NiFTI images to model
        :param opts: the  workflow options
        :return: the :meth:`qipipe.pipeline.modeling.run` result
        """
        # Run the workflow.
        result = modeling.run(project, subject, session, scan, *images,
                              **opts)
        # Find the modeling resource.
        rsc = xnat.find_one(project, subject, session, scan=scan,
                            resource=result)
        assert_is_not_none(rsc, "The %s %s %s XNAT modeling resource was not"
                                " created" % (sbj, sess, result))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
