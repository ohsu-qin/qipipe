import os
import glob
from nose.tools import *

import sys
from qipipe.helpers import dicom_helper as dcm

FIXTURE = os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'helpers', 'edit_dicom')
"""The test data."""

SBJ_ID = '111710'
"""The Subject ID for the test images."""

STUDY_ID = '1'
"""The Study ID for the test images."""

STUDY_UID = '1.3.12.2.1107.5.2.32.35139.30000010111521270056200000038'
"""The Study UID for the test images."""

SERIES_NBR = 7
"""The Series Number for the test images."""

SERIES_UID = '1.3.12.2.1107.5.2.32.35139.2010111713034928807078745.0.0.0'
"""The Series UID for the test images."""


class TestDicomHelper:
    """dicom_helper unit tests."""

    def test_read_dicom_tags(self):
        # The first brain image.
        files = glob.glob(FIXTURE + '/*')
        # Read the tags.
        for ds in dcm.iter_dicom_headers(FIXTURE):
            tdict = dcm.select_dicom_tags(ds, 'Study ID', 'Series Number')
            study = tdict['Study ID']
            assert_equal(STUDY_ID, study, "Study tag incorrect: %s" % study)
            series = tdict['Series Number']
            assert_equal(SERIES_NBR, series, "Series tag incorrect: %d" % series)

    def test_read_image_hierarchy(self):
        for i, h in enumerate(dcm.read_image_hierarchy(FIXTURE)):
            assert_equal(SBJ_ID, h[0], "Subject ID incorrect: %s" % h[0])
            assert_equal(STUDY_UID, h[1], "Study UID incorrect: %s" % h[1])
            assert_equal(SERIES_UID, h[2], "Series UID incorrect: %s" % h[2]) 
            assert_equal(i+5, h[3], "Instance Number incorrect: %s" % h[3]) 


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
