import os
from nose.tools import assert_equal
from qiutil.collections import tuplize
from qipipe.helpers import roi
from ... import ROOT

# matplotlib plot induces a FutureWarning.
import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)


FIXTURE = os.path.join(ROOT, 'fixtures', 'staged', 'breast', 'Breast003',
                       'Session01', 'scans', '1', 'resources', 'roi',
                       'roi.nii.gz') 

class TestROI(object):
    
    def setup(self):
        self.roi = roi.load(FIXTURE, [1, 1, 1.2])
    
    def test_extent(self):
        # Compare to the expected extent.
        expected = (((85, 210, 56), (96, 264, 60)),
                    ((116, 228, 53), (66, 235, 56)),
                    ((95, 262, 47), (67, 224, 63)))
        actual = tuplize(self.roi.extent.segments)
        # assert_equal(actual, expected,
        #              "ROI extent is incorrect: " + str(actual))
        
        self.roi.extent.show()


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
