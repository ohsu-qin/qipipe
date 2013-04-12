from nose.tools import *
import os
from base64 import b64encode as encode
from qipipe.helpers import xnat_helper

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'helpers', 'xnat', 'dummy.nii.gz')
"""The test fixture parent directory."""

SUBJECT = 'Test_' + encode('XNATHelper').strip('=')
"""The test subject label."""

class TestXNATHelper:
    """XNAT helper unit tests."""
    
    def setUp(self):
        self.xnat = xnat_helper.facade()
        self._delete_test_subject()
        
    def tearDown(self):
        self._delete_test_subject()
        
    def test_upload(self):
        session = SUBJECT + '_MR1'
        self.xnat.upload('QIN', SUBJECT, session, FIXTURE, scan=1, modality='MR')
        _, fname = os.path.split(FIXTURE)
        f = self.xnat.interface.select('/project/QIN').subject(SUBJECT).experiment(session).scan('1').resource('NIFTI').file(fname)
        assert_true(f.exists(), "File not uploaded: " + fname)
    
    def _delete_test_subject(self):
        """Deletes the test C{QIN} L{SUBJECT}."""
        
        sbj = self.xnat.interface.select('/project/QIN/subject/' + SUBJECT)
        if sbj.exists():
            sbj.delete()

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
