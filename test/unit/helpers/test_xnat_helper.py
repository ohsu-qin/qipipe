from nose.tools import *
from base64 import b64encode as encode
import pyxnat

import logging
logger = logging.getLogger(__name__)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import qipipe.helpers.xnat_helper as xnat

LABEL = 'Test_' + encode('XNATHelper').replace('=', '')
"""The test subject label."""

class TestXNATHelper:
    """XNAT helper unit tests."""
    
    def setUp(self):
        self.xnat = pyxnat.Interface(config=xnat.config())
        s = self.xnat.select('/project/QIN/subject/' + LABEL)
        if s.exists():
            s.delete()
        
    def tearDown(self):
        s = self.xnat.select('/project/QIN/subject/' + LABEL)
        if s.exists():
            s.delete()
        
    def test_subject_id_for_label(self):
        result = xnat.subject_id_for_label(project='QIN', label=LABEL)
        assert_is_none(result, "Subject id found for nonexistent label %s" % LABEL)
        subject_id = xnat.subject_id_for_label(project='QIN', label=LABEL, create=True)
        assert_is_not_none(subject_id, "Subject not created: %s" % LABEL)
        result = xnat.subject_id_for_label(project='QIN', label=LABEL)
        assert_equal(subject_id, result, "Subject not found: %s" % LABEL)

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
