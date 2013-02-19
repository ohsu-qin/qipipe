from nose.tools import *
import pyxnat

import logging
logger = logging.getLogger(__name__)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.helpers.dicom_helper import iter_dicom
from qipipe.pipelines.xnat import XNAT_CFG, subject_id_for_label

import pyxnat
from nipype.interfaces.io import XNATSink
import time

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'pipeline')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'pipeline')

class TestXNAT:
    """Pipeline XNAT helper unit tests."""
    
    # def setUp(self):
    #     self.xnat = pyxnat.Interface(config=XNAT_CFG)
    #     s = self.xnat.select('/project/QIN/subject/' + LABEL)
    #     if s.exists():
    #         s.delete()
    #     
    # def tearDown(self):
    #     s = self.xnat.select('/project/QIN/subject/' + LABEL)
    #     if s.exists():
    #         s.delete()
        
    # def test_subject_id_for_label(self):
    #     subject_id_for_label.inputs.project = 'QIN'
    #     subject_id_for_label.inputs.label = LABEL
    #     result = subject_id_for_label.run()
    #     assert_is_none(result.outputs.subject_id, "Subject id found for nonexistent label %s" % LABEL)
    #     subject_id_for_label.inputs.create = True
    #     result = subject_id_for_label.run()
    #     assert_is_not_none(result.outputs.subject_id, "Subject not created: %s" % LABEL)
    #     subject_id_for_label.inputs.create = False
    #     result = subject_id_for_label.run()
    #     assert_is_not_none(result.outputs.subject_id, "Subject not found: %s" % LABEL)
        
    def test_store_image(self):
        xnat = XNATSink(input_names=['in_file'])
        xnat.inputs.config = XNAT_CFG
        xnat.inputs.project_id = 'QIN'
        xnat.inputs.experiment_id = 'TestExperiment'
        xnat.inputs.subject_id = LABEL
        xnat.inputs.share = True
        xnat.inputs.in_file = glob.glob(RESULTS + 'Subj_1/Visit_1/')[ds.filename for ds in iter_dicom(FIXTURE)][0]
        xnat.run()

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
