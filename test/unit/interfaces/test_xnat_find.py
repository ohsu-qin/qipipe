import os, glob, re, shutil
from nose.tools import (assert_equal, assert_false, assert_true)
from nipype.interfaces.traits_extension import isdefined
from qipipe.helpers.logging_helper import logger
from qipipe.interfaces import XNATFind
from qipipe.helpers import xnat_helper
from qipipe.helpers import xnat_helper
from test.helpers.project import project
from test.unit.helpers.test_xnat_helper import (FIXTURES, RESULTS)
from test.helpers.xnat_test_helper import generate_subject_name

SUBJECT = generate_subject_name(__name__)
"""The test subject name."""

SESSION = 'MR1'
"""The test session name."""

SCAN = 1
"""The test scan number."""

RECON = 'reg'
"""The test reconstruction name."""

ASSESSOR = 'pk'
"""The test assessor name."""


class TestXNATFind(object):
    """The XNAT find interface unit tests."""
        
    def setUp(self):
        xnat_helper.delete_subjects(project(), SUBJECT)
        
    def tearDown(self):
        xnat_helper.delete_subjects(project(), SUBJECT)
        shutil.rmtree(RESULTS, True)
    
    def test_find_subject(self):
        self._test_find(SUBJECT)
    
    def test_find_session(self):
        self._test_find(SUBJECT, SESSION)
    
    def test_find_scan(self):
        self._test_find(SUBJECT, SESSION, scan=SCAN)
    
    def test_find_resource(self):
        self._test_find(SUBJECT, SESSION, scan=SCAN, resource='NIFTI')
    
    def test_find_reconstruction(self):
        self._test_find(SUBJECT, SESSION, reconstruction=RECON)
    
    def test_find_assessor(self):
        self._test_find(SUBJECT, SESSION, assessor=ASSESSOR)
    
    def _test_find(self, *args, **opts):
        logger(__name__).debug("Testing the XNATFind interface on %s %s..." %
            (args, opts))
        
        # Add the arguments to the inputs.
        inputs = opts
        args = list(args)
        inputs['subject'] = args[0]
        if len(args) > 1:
            inputs['session'] = args[1]
        
        # Try to find the object.
        find = XNATFind(project=project(), **inputs)
        result = find.run()
        assert_false(isdefined(result.outputs.xnat_id),
            "Find %s incorrectly returned an id: %s." %
            (inputs, result.outputs.xnat_id))
        
        # Create the object.
        find = XNATFind(project=project(), create=True, **inputs)
        result = find.run()
        assert_true(isdefined(result.outputs.xnat_id),
            "Find %s with create did not return an id." % inputs)
        xnat_id = result.outputs.xnat_id
        
        # Refind the object.
        find = XNATFind(project=project(), **inputs)
        result = find.run()
        assert_equal(result.outputs.xnat_id, xnat_id,
            "Find %s returned the wrong id: %s." %
            (inputs, result.outputs.xnat_id))
        

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
