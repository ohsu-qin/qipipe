import os
from nose.tools import (assert_equal, assert_not_equal, assert_is_not_none)
from qiutil.collections import tuplize
from qipipe.helpers import roi
from ... import ROOT

FIXTURE = os.path.join(ROOT, 'fixtures', 'staged', 'breast', 'Breast003',
                       'Session01', 'scans', '1', 'resources', 'roi',
                       'roi.nii.gz')


class TestROI(object):
    
    def setup(self):
        self.roi = roi.load(FIXTURE, [1, 1, 1.2])
    
    def test_segments(self):
        expected = (((85, 210, 56), (96, 264, 60)),
                    ((105, 235, 37), (83, 245, 73)),
                    ((112, 232, 67), (70, 243, 47)))
        actual = tuplize(self.roi.extent.segments)
        assert_equal(actual, expected, "ROI segments are incorrect: %s" %
                                       str(actual))
    
    def test_volume(self):
        volume = self.roi.extent.volume
        assert_equal(volume, 57001.2, "ROI volume is incorrect: %f" % volume)
    
    def test_slices(self):
        slices = self.roi.slices
        assert_is_not_none(slices)
        assert_not_equal(len(slices), 0, "ROI slices are missing")
        assert_equal(len(slices), 40, "ROI slice count is incorrect: %d" %
                                      len(slices))
        maximal = self.roi.maximal_slice_index()
        assert_equal(maximal, 20, "ROI maximal slice is incorrect: %d" %
                                  maximal)
        z, extent = slices[maximal]
        assert_equal(z, 56, "ROI maximal slice z value is incorrect: %d" % z)
        assert_equal(extent.area, 1933, "ROI maximal slice volume is"
                                       " incorrect: %f" % extent.area)
        
        # Work around the following bug:
        # * extent.show() triggers a matplotlib plot function FutureWarning.
        #   The work-around is to filter this warning in a context.
        #
        # Uncomment to display the ROI convex hull.
        import warnings
        with warnings.catch_warnings():
           warnings.simplefilter(action='ignore', category=FutureWarning)
           self.roi.extent.show()


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
