from nose.tools import *
import os, glob, shutil

import logging
logger = logging.getLogger(__name__)

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'pipeline', 'stage')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'stage')

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS))
config.update_config(cfg)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines.qipipeline import qipipeline as qip
from qipipe.helpers.dicom_helper import iter_dicom

import pyxnat 

class TestPipeline:
    """Pipeline unit tests."""
    
    def test_pipeline(self):
        #pyxnat.interfaces.DEBUG = True
        #pyxnat.resources.DEBUG = True
        shutil.rmtree(RESULTS, True)
        qip.inputs.collection = 'Sarcoma'
        qip.inputs.dest = RESULTS
        for d in glob.glob(os.path.join(FIXTURE, 'Subj*')):
            logger.debug("Testing QIN pipeline on %s..." % d)
            qip.inputs.patient_dir = d
            qip.run()
        # Verify the result.
        
        # Cleanup.
        #shutil.rmtree(RESULTS, True)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
