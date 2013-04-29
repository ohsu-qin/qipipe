from nose.tools import *
import sys, os, shutil
from qipipe.helpers import xnat_helper

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.xnat_test_helper import generate_subject_label, delete_subjects

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'helpers', 'xnat', 'dummy.nii.gz')
"""The test fixture parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'helpers', 'xnat')
"""The test results directory."""

SUBJECT = generate_subject_label(__name__)
"""The test subject label."""


class TestXNATHelper(object):
    """The XNAT helper unit tests."""
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
        delete_subjects(SUBJECT)
        
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
        delete_subjects(SUBJECT)
        
    def test_upload_and_download(self):
        session = SUBJECT + '_MR1'
        with xnat_helper.connection() as xnat:
            # Upload the file.
            xnat.upload('QIN', SUBJECT, session, FIXTURE, scan=1, modality='MR', format='NIFTI')
            _, fname = os.path.split(FIXTURE)
            sbj = xnat.interface.select('/project/QIN').subject(SUBJECT)
            file_obj = sbj.experiment(session).scan('1').resource('NIFTI').file(fname)
            assert_true(file_obj.exists(), "File not uploaded: %s" % fname)
            
            # Download the uploaded file.
            files = list(xnat.download('QIN', sbj.label(), session, dest=RESULTS, scan=1, format='NIFTI'))
        assert_not_equal(0, len(files), "No files were downloaded")
        assert_equal(1, len(files), "Too many files were downloaded: %s" % files)
        f = files[0]
        assert_true(os.path.exists(f), "File not downloaded: %s" % f)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
