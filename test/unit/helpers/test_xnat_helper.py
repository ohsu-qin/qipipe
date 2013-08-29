from nose.tools import *
import sys, os, shutil
from qipipe.helpers import xnat_helper
from test import ROOT
from test.helpers.project import project
from test.helpers.xnat_test_helper import (delete_subjects, generate_subject_name)

FIXTURES = os.path.join(ROOT, 'fixtures', 'helpers', 'xnat')
"""The test fixture parent directory."""

FIXTURE = os.path.join(FIXTURES, 'dummy_scan.nii.gz')
"""The test fixture."""

RESULTS = os.path.join(ROOT, 'results', 'helpers', 'xnat')
"""The test results directory."""

SUBJECT = generate_subject_name(__name__)
"""The test subject name."""

SESSION = SUBJECT + '_MR1'

SCAN = 1

class TestXNATHelper(object):
    """The XNAT helper unit tests."""
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
        delete_subjects(project(), SUBJECT)
        
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
        delete_subjects(project(), SUBJECT)
        
    def test_round_trip(self):
        with xnat_helper.connection() as xnat:
            # Upload the file.
            xnat.upload(project(), SUBJECT, SESSION, FIXTURE, scan=SCAN, modality='MR',
                format='NIFTI')
            _, fname = os.path.split(FIXTURE)
            scan_obj = xnat.get_scan(project(), SUBJECT, SESSION, SCAN)
            file_obj = scan_obj.resource('NIFTI').file(fname)
            assert_true(file_obj.exists(), "File not uploaded: %s" % fname)
            
            # Download the single uploaded file.
            files = xnat.download(project(), SUBJECT, SESSION, dest=RESULTS, scan=SCAN,
                format='NIFTI')
            # Download all scan files.
            all_files = xnat.download(project(), SUBJECT, SESSION, dest=RESULTS,
                container_type='scan', format='NIFTI')
            
        # Verify the result.
        assert_equal(1, len(files), "The download file count is incorrect: %d" % len(files))
        f = files[0]
        assert_true(os.path.exists(f), "File not downloaded: %s" % f)
        assert_equal(set(files), set(all_files),
            "The %s %s scan %d download differs from all scans download: %s vs %s" %
                (SUBJECT, SESSION, SCAN, files, all_files))
        
        # Work around pyxnat bug by reconnecting in order to get the accurate experiment
        # existence status.
        with xnat_helper.connection() as xnat:
            exp = xnat.get_session(project(), SUBJECT, SESSION)
            assert_true(exp.exists(), "XNAT %s %s %s experiment does not exist." %
                (project(), SUBJECT, SESSION))
            # Test replace.
            xnat.upload(project(), SUBJECT, SESSION, FIXTURE, scan=SCAN, modality='MR',
                format='NIFTI', overwrite=True)
        


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
