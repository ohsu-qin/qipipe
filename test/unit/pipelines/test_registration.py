import sys, os, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'registration', 'breast')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'registration')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)
from nipype.interfaces.utility import IdentityInterface

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import registration as reg
from test.unit.pipelines.pipelines_helper import get_xnat_subjects, clear_xnat_subjects

class TestRegistrationPipeline:
    """Registration pipeline unit tests."""
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
        # The registration-only workflow.
        self.wf = self._create_workflow()
        # The XNAT interface.
        self.xnat = XNAT().interface
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_registration(self):
        """
        Run the registration pipeline and verify that the registered images are created
        in XNAT.
        """
        
        logger.debug("Testing the registration pipeline on %s..." % FIXTURE)

        # The test subject => directory dictionary.
        sbj_dir_dict = get_xnat_subjects(collection, src)
        # Delete any existing test subjects.
        clear_xnat_subjects(sbj_dir_dict.iterkeys())

        # Run the pipeline.
        logger.debug("Executing the registration pipeline...")
        self.wf.run()

        # Verify the result.
        for sbj in sbj_dir_dict.iterkeys():
            assert_true(sbj.exists(), "Subject not created in XNAT: %s" % sbj_id)
        
        # Cleanup.
        for sbj in subjects:
            sbj.delete(delete_files=True)
    
    def _create_workflow(self):
        work_dir = os.path.join(RESULTS, 'work')
        os.makedirs(work_dir)
        wf = pe.Workflow(name='registration', base_dir=work_dir)
        # The workflow input.
        input_spec = pe.Node(IdentityInterface(fields=['in_files']), name='input_spec')
        wf.connect(reg.create_registration_connections(input_spec, 'in_files'))
        return wf


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
