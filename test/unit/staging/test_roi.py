import os
from nose.tools import (assert_equal, assert_is_not_none)
from collections import defaultdict
from ... import ROOT
from qipipe.staging import collections
from qipipe.staging.roi import iter_roi
from ...helpers.logging import logger

COLLECTION = 'Breast'
"""The test collection."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging', 'breast', 'BreastChemo3')
"""The test fixture subject directory."""

SUBJECT = 'Breast001'
"""The test subject."""


class TestROI(object):
    """ROI iteration unit tests."""
    
    def setUp(self):
        collection = collections.with_name(COLLECTION)
        self.patterns = collection.patterns.scan[1].roi
    
    def test_multi_lesion(self):
        multi_lesion_visit = os.path.join(FIXTURES, 'Visit1')
        rois = list(iter_roi(self.patterns.glob, self.patterns.regex,
                             multi_lesion_visit))
        assert_equal(len(rois), 4, "The multi-lesion ROI file count is"
                                   " incorrect: %d" % len(rois))
        roi_grps = defaultdict(dict)
        for roi in rois:
            roi_grps[roi.lesion][roi.slice] = roi.path
        expected_lesions = {1, 2}
        expected_slice_indexes = {12, 13}
        lesions = set(roi_grps.iterkeys())
        assert_equal(lesions, expected_lesions, "The multi-lesion ROI lesion"
                                                " numbers are incorrect: %s" %
                                                lesions)
        for lesion, slice_dict in roi_grps.iteritems():
            assert_equal(set(slice_dict.iterkeys()), expected_slice_indexes,
                         "The multi-lesion ROI slice indexes are incorrect")
            for path in slice_dict.itervalues():
                _, fname = os.path.split(path)
                assert_equal(fname, 'roi.bqf', "The multi-lesion ROI file name"
                                               " is incorrect: %s" % fname)

    def test_single_lesion(self):
        single_lesion_visit = os.path.join(FIXTURES, 'Visit2')
        rois = list(iter_roi(self.patterns.glob, self.patterns.regex,
                             single_lesion_visit))
        assert_equal(len(rois), 2, "The single lesion ROI file count is"
                                   " incorrect: %d" % len(rois))
        roi_grps = defaultdict(dict)
        for roi in rois:
            roi_grps[roi.lesion][roi.slice] = roi.path
        expected_lesions = {1}
        expected_slice_indexes = {12, 13}
        lesions = set(roi_grps.iterkeys())
        assert_equal(lesions, expected_lesions, "The single lesion ROI lesion"
                                                " numbers are incorrect: %s" %
                                                lesions)
        for lesion, slice_dict in roi_grps.iteritems():
            
            slice_indexes = set(slice_dict.iterkeys())
            assert_equal(slice_indexes, expected_slice_indexes,
                         "The single lesion ROI slice indexes are incorrect: %s" %
                         slice_indexes)
            for path in slice_dict.itervalues():
                _, fname = os.path.split(path)
                assert_equal(fname, 'roi.bqf', "The single lesion ROI file name"
                                               " is incorrect: %s" % fname)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
