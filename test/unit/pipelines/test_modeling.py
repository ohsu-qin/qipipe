import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import modeling
from qipipe.helpers import xnat_helper
from test.helpers.project import project
from test.unit.pipelines.test_registration import ROOT, FIXTURES, TestRegistrationWorkflow

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'analysis')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestModelingWorkflow(TestRegistrationWorkflow):
    """
    Modeling workflow unit tests.
    
    This test exercises the modeling workflow on three series of one visit in each of the
    Breast and Sarcoma studies.
    """

    def __init__(self):
        """Sets the ``_logger`` instance variable."""
        self._logger = logger
    
    def _run_workflow(self, *session_specs, **opts):
        # Collect all session scans as dictionaries. Each dictionary has
        # subject, session and scan key => value items.
        scan_dicts = []
        with xnat_helper.connection() as xnat:
            for sbj, sess in session_specs:
                sess_obj = xnat.get_session(project=project(), subject=sbj, session=sess)
                for scan_obj in sess_obj.scans():
                    scan_dict = dict(subject=sbj, session=sess, scan=scan_obj.label())
                    scan_dicts.append(scan_dict)
        
        # Run the workflow.
        return modeling.run(*scan_dicts, **opts)
    
    def _verify_result(self, xnat, sess_files_dict, *analysis_specs):
        sess_anl_dict = {(sbj, sess): anl for sbj, sess, anl in analysis_specs}
        for spec, in_files in sess_files_dict.iteritems():
            assert_in(spec, sess_anl_dict, "The session %s %s was not modeled" % spec)
            anl = sess_anl_dict[spec]
            sbj, sess = spec
            anl_obj = xnat.get_analysis(project(), sbj, sess, anl)
            assert_true(anl_obj.exists(),
                "The %s %s %s XNAT analysis object was not created" % (sbj, sess, anl))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
