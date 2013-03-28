import os
import glob
from nose.tools import *

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.helpers import dicom_helper as dcm

# The test fixture.
FIXTURES = os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures')

FIXTURE = os.path.join(FIXTURES, 'brain')

# The subject-study-series for the 20 images.
SBJ_ID = '123565'
"""The Subject ID for the test images."""

STUDY_UID = '0.0.0.0.2.8811.20010413115754.12432'
"""The Study UID for the test images."""

SERIES_UID = '0.0.0.0.3.8811.2.20010413115754.12432'
"""The Series UID for the test images."""


class TestDicomHelper:
    """dicom_helper unit tests."""

    def test_read_dicom_tags(self):
        # The first brain image.
        files = glob.glob(FIXTURE + '/*')
        # Read the tags.
        for ds in dcm.iter_dicom_headers(*files):
            tdict = dcm.select_dicom_tags(ds, 'Study ID', 'Series Number')
            study = tdict['Study ID']
            assert_equal('8811', study, "Study tag incorrect: %s" % study)
            series = tdict['Series Number']
            assert_equal(2, series, "Series tag incorrect: %d" % series)

    def test_read_image_hierarchy(self):
        for i, h in enumerate(dcm.read_image_hierarchy(FIXTURE)):
            assert_equal(SBJ_ID, h[0], "Subject ID incorrect: %s" % h[0])
            assert_equal(STUDY_UID, h[1], "Study UID incorrect: %s" % h[1])
            assert_equal(SERIES_UID, h[2], "Series UID incorrect: %s" % h[2]) 
            assert_equal(i+1, h[3], "Instance Number incorrect: %s" % h[3]) 


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
