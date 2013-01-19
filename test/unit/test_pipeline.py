from nose.tools import *
import os, glob, shutil

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe import pipeline
from qipipe.helpers.dicom_helper import iter_dicom

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'pipeline', 'sarcoma', 'patient03')
# The test results.
RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'sarcoma', 'patient03')

class TestPipeline:
    """Pipeline unit tests."""
    
    def test_pipeline(self):
        shutil.rmtree(RESULTS, True)
        pipeline.inputs.fix_dicom.source = FIXTURE
        pipeline.inputs.fix_dicom.dest = RESULTS
        pipeline.run()
        # Verify the result.
        for ds in iter_dicom(RESULTS):
            assert_equal('CHEST', ds.BodyPartExamined, "Incorrect Body Part: %s" % ds.BodyPartExamined)
            assert_equal('Sarcoma03', ds.PatientID, "Incorrect Patient ID: %s" % ds.PatientID)
        # Cleanup.
        shutil.rmtree(RESULTS, True)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
