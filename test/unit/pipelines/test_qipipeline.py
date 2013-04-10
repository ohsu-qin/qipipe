import os, sys, shutil
from nose.tools import *
import tempfile

import logging
logger = logging.getLogger(__name__)

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'group_dicom')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'qipipeline')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import qipipeline as qip
from qipipe.pipelines import QIPipeline
from qipipe.helpers.dicom_helper import iter_dicom
from qipipe.helpers.xnat_helper import XNAT
from qipipe.staging import airc_collection as airc
from test.unit.pipelines.pipelines_helper import get_xnat_subjects, clear_xnat_subjects

class TestPipeline:
    """Pipeline unit tests."""
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
        self.xnat = XNAT().interface
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
    
    def test_breast(self):
        self._test_collection('Breast')
    
    def test_sarcoma(self):
        self._test_collection('Sarcoma')
    
    def _test_collection(self, collection):
        """
        Run the pipeline on the given collection and verify the following:
            - scans are created in XNAT
        
        @param collection: the AIRC collection name
        @attention: This test does not verify the CTP staging area nor that the
        image files are correctly uploaded. These features should be
        verified manually.
        """

        airc_coll = airc.collection_with_name(collection)
        src = os.path.join(FIXTURE, collection.lower())
        logger.debug("Testing the QIN pipeline on %s..." % src)

        # The test subject => directory dictionary.
        sbj_dir_dict = get_xnat_subjects(collection, src)
        # Delete any existing test subjects.
        clear_xnat_subjects(sbj_dir_dict.iterkeys())

        # Run the pipeline.
        dest = os.path.join(RESULTS, collection.lower())
        work_dir = tempfile.mkdtemp()
        logger.debug("Executing the QIN pipeline in %s..." % work_dir)
        sessions = qip.run(collection, dest=dest, base_dir=work_dir,
            components = [QIPipeline.STAGING, QIPipeline.STACK], *sbj_dir_dict.itervalues())
        
        # Verify the result.
        for sess_nm in sessions:
            sess = self.xnat.select('/project/QIN/experiment/' + sess_nm)
            assert_true(sess.exists(), "Session not created in XNAT: %s" % sess_nm)
            sbj_id = sess.attrs.get('subject_ID')
            assert_is_not_none(sbj_id, "Session does not have a subject: %s" % sess_nm)
            scans = self.xnat.select('/project/QIN/subject/' + sbj_id + '/experiment/' + sess_nm + '/scans').get()
            assert_not_equal(0, len(scans), "Session does not scans: %s" % sess_nm)
        
        # Cleanup.
        clear_xnat_subjects(sbj_dir_dict.iterkeys())
        shutil.rmtree(work_dir, True)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
