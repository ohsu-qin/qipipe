from nose.tools import *
import sys, os, glob, re, shutil
from nipype.interfaces.traits_extension import isdefined

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.project import project
from qipipe.interfaces import XNATFind
from qipipe.helpers import xnat_helper
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
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

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestXNATFind(object):
    """The XNAT find interface unit tests."""
        
    def setUp(self):
        delete_subjects(project(), SUBJECT)
        
    def tearDown(self):
        delete_subjects(project(), SUBJECT)
        shutil.rmtree(RESULTS, True)
    
    def test_find_subject(self):
        self._test_find(SUBJECT)
    
    def test_find_session(self):
        self._test_find(SUBJECT, SESSION)
    
    def test_find_scan(self):
        self._test_find(SUBJECT, SESSION, scan=SCAN)
    
    def test_find_reconstruction(self):
        self._test_find(SUBJECT, SESSION, reconstruction=RECON)
    
    def test_find_assessor(self):
        self._test_find(SUBJECT, SESSION, assessor=ASSESSOR)
    
    def _test_find(self, *args, **opts):
        logger.debug("Testing the XNATFind interface on %s %s..." %
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
        assert_false(isdefined(result.outputs.label),
            "Find %s incorrectly returned a label: %s." %
            (inputs, result.outputs.label))
        
        # Create the object.
        find = XNATFind(project=project(), create=True, **inputs)
        result = find.run()
        assert_true(isdefined(result.outputs.label),
            "Find %s with create did not return a label." % inputs)
        label = result.outputs.label
        
        # Refind the object.
        find = XNATFind(project=project(), **inputs)
        result = find.run()
        assert_equals(result.outputs.label, label,
            "Find %s returned the wrong label: %s." %
            (inputs, result.outputs.label))
        

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)