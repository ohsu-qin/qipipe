import os, glob, re, shutil
from nose.tools import (assert_equal, assert_false, assert_true)
from nipype.interfaces.traits_extension import isdefined
from qipipe.interfaces import XNATFind
import qixnat
from ... import (project, ROOT)
from ...helpers.logging import logger
from ...helpers.name_generator import generate_unique_name

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'xnat', 'Sarcoma001',
                      'Session01', 'scans', '1', 'resources', 'NIFTI',
                      'volume001.nii.gz')
"""The test fixtures directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'xnat')
"""The test results directory."""

SUBJECT = generate_unique_name(__name__)
"""The test subject name."""

SESSION = 'Session01'
"""The test session name."""

SCAN = 9
"""The test scan number."""

RECONSTRUCTION = 'reco'
"""The test reconstruction name."""

ASSESSOR = 'pk'
"""The test assessor name."""


class TestXNATFind(object):
    """The XNAT find interface unit tests."""

    def setUp(self):
        with qixnat.connect() as xnat:
            xnat.delete_subjects(project(), SUBJECT)
        
    def tearDown(self):
        with qixnat.connect() as xnat:
            xnat.delete_subjects(project(), SUBJECT)
        shutil.rmtree(RESULTS, True)
    
    def test_find_subject(self):
        self._test_find(SUBJECT)
    
    def test_find_session(self):
        self._test_find(SUBJECT, SESSION)
    
    def test_find_scan(self):
        self._test_find(SUBJECT, SESSION, scan=SCAN)
    
    def test_find_experiment_resource(self):
        self._test_find(SUBJECT, SESSION, resource='reg')
    
    def test_find_scan_resource(self):
        self._test_find(SUBJECT, SESSION, scan=SCAN, resource='NIFTI')
    
    def test_find_scan_file(self):
        self._test_find(SUBJECT, SESSION, scan=SCAN, resource='NIFTI',
                        file=FIXTURE)
    
    def test_find_reconstruction(self):
        self._test_find(SUBJECT, SESSION, reconstruction=RECONSTRUCTION)
    
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
        find = XNATFind(project=project(), modality='MR', create=True, **inputs)
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
