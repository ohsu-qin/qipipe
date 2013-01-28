from nose.tools import *
import os, glob, shutil

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'pipeline', 'sarcoma', 'patient03')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'sarcoma', 'patient03')

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS))
config.update_config(cfg)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe import pipeline
from qipipe.helpers.dicom_helper import iter_dicom

class TestPipeline:
    """Pipeline unit tests."""
    
    def test_pipeline(self):
        shutil.rmtree(RESULTS, True)
        pipeline.inputs.group.source = FIXTURE
        pipeline.inputs.group.dest = GROUPED
        pipeline.inputs.fix.collection = 'Sarcoma'
        pipeline.inputs.fix.dest = WORK
        pipeline.inputs.assemble.base_directory = STACKED
        pipeline.run()
        # Verify the result.
        for ds in iter_dicom(OUTPUT):
            assert_equal('CHEST', ds.BodyPartExamined, "Incorrect Body Part: %s" % ds.BodyPartExamined)
            assert_equal('Sarcoma03', ds.PatientID, "Incorrect Patient ID: %s" % ds.PatientID)
        # Cleanup.
        #shutil.rmtree(RESULTS, True)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
