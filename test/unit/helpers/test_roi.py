import os
from qipipe.helpers import roi
from ... import ROOT

FIXTURE = os.path.join(ROOT, 'fixtures', 'staged', 'breast', 'Breast003',
                       'Session01', 'scan', '1', 'resources', 'roi',
                       'roi.nii.gz') 

class TestROI(object):
    
    def setup(self):
        self.roi = roi.load(FIXTURE)
    
    def test_extent(self):
        