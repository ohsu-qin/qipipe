import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.project import project
from qipipe.pipelines import registration
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from test.unit.pipelines.xnat_scan_test_base import XNATScanTestBase, ROOT

FIXTURES = os.path.join(ROOT, 'fixtures', 'registration')
"""The test fixtures directory."""

REG_CONF = os.path.join(ROOT, 'conf', 'registration.cfg')
"""The test registration configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'registration')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestRegistrationWorkflow(XNATScanTestBase):
    """
    Registration workflow unit tests.
    
    This test exercises the registration workflow on three series of one visit in each of the
    Breast and Sarcoma studies.
    """
    
    def __init__(self):
        super(TestRegistrationWorkflow, self).__init__(FIXTURES, RESULTS)
    
    def _run_workflow(self, xnat, fixture, *inputs, **opts):
        """
        Executes :meth:`qipipe.pipelines.registration.run` on the input sessions.
        
        :param xnat: the :class:`qipipe.helpers.xnat_helpers.XNAT` connection
        :param fixture: the test fixture directory
        :param inputs: the (subject, session) tuples
        :param opts: the :meth:`qipipe.pipelines.modeling.run` options
        :return: the :meth:`qipipe.pipelines.modeling.run` result
        """
        logger.debug("Testing the registration workflow on %s..." % fixture)
        return registration.run(*inputs, config=REG_CONF, **opts)
    
    def _verify_result(self, xnat, inputs, result):
        sess_recon_dict = {(sbj, sess): recon for sbj, sess, recon in result}
        for spec, in_files in inputs.iteritems():
            assert_in(spec, result, "The session %s %s was not registered" % spec)
            recon = sess_recon_dict[spec]
            sbj, sess = spec
            recon_obj = xnat.get_reconstruction(project(), sbj, sess, recon)
            assert_true(recon_obj.exists(),
                "The %s %s %s XNAT reconstruction object was not created" % (sbj, sess, recon))
            recon_files = recon_obj.out_resource('NIFTI').files().get()
            assert_equals(len(in_files), len(recon_files),
                "The registered %s %s file count is incorrect - expected: %d, found: %d" %
                (sbj, sess, len(in_files), len(recon_files)))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
