import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import registration as reg
from qipipe.helpers import xnat_helper
from test.helpers.xnat_test_helper import generate_subject_label, delete_subjects

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'stacks', 'breast')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'registration')
"""The test results directory."""

SUBJECT = generate_subject_label(__name__)
"""The test subject label."""

SESSION = "%s_%s" % (SUBJECT, 'Session01')

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestRegistrationPipeline:
    """Registration pipeline unit tests."""
    
    def setUp(self):
        delete_subjects(SUBJECT)
        shutil.rmtree(RESULTS, True)
        # The XNAT test session.
        with xnat_helper.connection() as xnat:
            sess = xnat.interface.select('/project/QIN').subject(SUBJECT).experiment(SESSION)
            # The test file objects.
            self._file_names = set()
            for f in glob.glob(FIXTURE + '/Breast*/Session*/series*.nii.gz'):
                _, fname = os.path.split(f)
                self._file_names.add(fname)
                scan = str(int(re.match('series(\d{3}).nii.gz', fname).group(1)))
                file_obj = sess.scan(scan).resource('NIFTI').file(fname)
                # Upload the file.
                file_obj.insert(f, experiments='xnat:MRSessionData', format='NIFTI')
        logger.debug("Uploaded the test %s files %s." % (SESSION, list(self._file_names)))
    
    def tearDown(self):
        #delete_subjects(SUBJECT)
        shutil.rmtree(RESULTS, True)

    def test_registration(self):
        """
        Run the registration workflow and verify that the registered images are created
        in XNAT.
        """
        
        logger.debug("Testing the registration workflow on %s..." % SUBJECT)

        # The staging work area.
        work = os.path.join(RESULTS, 'work')

        # Run the workflow.
        recon_specs = reg.run((SUBJECT, SESSION), base_dir=work)

        # Verify the result.
        assert_equals(1, len(recon_specs), "Resampled XNAT file count incorrect: %s" % len(recon_specs))
        with xnat_helper.connection() as xnat:
            recon = xnat.get_reconstruction('QIN', *recon_specs[0])
            rsc = recon.out_resource('NIFTI')
            assert_true(rsc.exists(), "Resource not created for %s" % recon.label())
            recon_files = list(rsc.files())
            assert_equals(2, len(recon_files), "Resampled XNAT file count incorrect: %s" % recon_files)
 

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
