import os
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import registration

class TestRegistrationPipeline:
    """Registration pipeline unit tests."""
    
    def setUp(self):
        self.wf = pe.Workflow(name='qipipeline', **opts)
        self.xnat = XNAT().interface
    
    def test_registration(self):
        """
        Run the registration pipeline and verify that the registered images are created
        in XNAT.
        """
        shutil.rmtree(RESULTS, True)
        logger.debug("Testing the registration pipeline on %s..." % FIXTURE)

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
        # Run the staging and stack pipeline.
        
        sessions = qip.run(collection, dest=dest, base_dir=work_dir,
            components = [QIPipeline.STAGING, QIPipeline.STACK], *sbj_dirs)

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
