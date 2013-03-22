from nose.tools import *
import os, re, glob, shutil

import logging
logger = logging.getLogger(__name__)

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'group_dicom')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'qipipeline')

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

import sys
import pyxnat
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import qipipeline as qip
from qipipe.helpers.dicom_helper import iter_dicom
from qipipe.helpers.xnat_helper import XNAT
from qipipe.staging import airc_collection as airc
from qipipe.staging.staging_helper import SUBJECT_FMT

class TestPipeline:
    """Pipeline unit tests."""
    
    def test_pipeline(self):
        """
        Run the pipeline and verify that scans are created in XNAT.
        This test does not verify the CTP staging area nor that the
        image files are correctly uploaded. These features are
        verified by visual inspection.
        """
        shutil.rmtree(RESULTS, True)
        self.xnat = XNAT().interface
        self._test_collection('Sarcoma')
        self._test_collection('Breast')
    
    def _test_collection(self, collection):
        airc_coll = airc.collection_with_name(collection)
        src = os.path.join(FIXTURE, collection.lower())
        logger.debug("Testing the QIN pipeline on %s..." % src)

        # The test subjects.
        subjects = []
        # The input directories.
        sbj_dirs = []
        for d in os.listdir(src):
            match = re.match(airc_coll.subject_pattern, d)
            if match:
                # The XNAT subject label.
                sbj_nm = SUBJECT_FMT % (collection, int(match.group(1)))
                logger.debug("Checking whether the test subject %s exists in XNAT..." % sbj_nm)
                # Delete the XNAT subject, if necessary.
                sbj = self.xnat.select('/project/QIN/subject/' + sbj_nm)
                subjects.append(sbj)
                if sbj.exists():
                    sbj.delete(delete_files=True)
                    logger.debug("Deleted the QIN pipeline test subject from XNAT: %s" % sbj_nm)
                sbj_dirs.append(os.path.join(src, d))
                logger.debug("Discovered QIN pipeline test subject subdirectory: %s" % d)

        # Run the pipeline.
        dest = os.path.join(RESULTS, collection.lower())
        work_dir = tempfile.mkdtemp()
        logger.debug("Executing the QIN pipeline in %s..." % work_dir)
        # Run the pipeline.
        sessions = qip.run(collection, dest=dest, base_dir=work_dir, *sbj_dirs)

        # Verify the result.
        for sess_nm in sessions:
            sess = self.xnat.select('/project/QIN/experiment/' + sess_nm)
            assert_true(sess.exists(), "Session not created in XNAT: %s" % sess_nm)
            sbj_id = sess.attrs.get('subject_ID')
            assert_is_not_none(sbj_id, "Session does not have a subject: %s" % sess_nm)
            scans = self.xnat.select('/project/QIN/subject/' + sbj_id + '/experiment/' + sess_nm + '/scans').get()
            assert_not_equal(0, len(scans), "Session does not scans: %s" % sess_nm)
        # Cleanup.
        for sbj in subjects:
            sbj.delete(delete_files=True)
        shutil.rmtree(RESULTS, True)
        shutil.rmtree(work_dir, True)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
