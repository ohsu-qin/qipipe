import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import modeling
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from test.helpers.project import project
from test.unit.pipelines.xnat_scan_test_base import XNATScanTestBase, ROOT

FIXTURES = os.path.join(ROOT, 'fixtures', 'pipelines', 'modeling')

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'modeling')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestModelingWorkflow(XNATScanTestBase):
    """
    Modeling workflow unit tests.
    This test exercises the modeling workflow on the QIN Breast and Sarcoma study
    visits in the ``test/fixtures/pipelines/modeling`` test fixture directory.
    
    Note:: a precondition for running this test is that the
        ``test/fixtures/pipelines/modeling`` directory contains the series stack
        test data in collection/subject/session format, e.g.::
    
            breast
                Breast003
                    Session01
                        series009.nii.gz
                        series023.nii.gz
                         ...
            sarcoma
                Sarcoma001
                    Session01
                        series011.nii.gz
                        series013.nii.gz
                         ...
    
    The fixture is not included in the Git source repository due to storage
    constraints.
    
    Note:: this test takes app. 8 hours to run on the AIRC cluster.
    """
    
    def __init__(self):
        super(TestModelingWorkflow, self).__init__(FIXTURES, RESULTS)
    
    def test_breast(self):
        self._test_breast()
    
    def test_sarcoma(self):
        self._test_sarcoma()
    
    def _run_workflow(self, xnat, fixture, *inputs, **opts):
        """
        Executes :meth:`qipipe.pipelines.modeling.run` on the input session scans.
        
        :param xnat: the :class:`qipipe.helpers.xnat_helpers.XNAT` connection
        :param fixture: the test fixture directory
        :param inputs: the (subject, session) tuples
        :param opts: the :meth:`qipipe.pipelines.modeling.run` options
        :return: the :meth:`qipipe.pipelines.modeling.run` result
        """
        logger.debug("Testing the modeling workflow on %s..." % fixture)
        # Run the workflow.
        return modeling.run(*inputs, **opts)
    
    def _verify_result(self, xnat, inputs, result):
        sess_anl_dict = {(sbj, sess): anl for sbj, sess, anl in result}
        for spec in inputs:
            assert_in(spec, sess_anl_dict, "The session %s %s was not modeled" % spec)
            anl = sess_anl_dict[spec]
            sbj, sess = spec
            anl_obj = xnat.get_analysis(project(), sbj, sess, anl)
            assert_true(anl_obj.exists(),
                "The %s %s %s XNAT analysis object was not created" % (sbj, sess, anl))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
