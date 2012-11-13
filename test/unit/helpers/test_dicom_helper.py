import os
import glob
from nose.tools import *

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.helpers import dicom_helper as dcm

# The test fixture.
FIXTURES = os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures')

FIXTURE = os.path.join(FIXTURES, 'brain')

# The patient-study-series-image hierarchy for the 20 images.
HIERARCHY = [['123565', '8811', '2', str(i)] for i in range(1, 21)]


class TestDicomHelper:
    """dicom_helper unit tests."""

    def test_read_dicom_tags(self):
        # The first brain image.
        files = glob.glob(FIXTURE + '/*')
        # Read the tags.
        tags = dcm.read_dicom_tags(['Study ID', 'Series Number'], *files)
        print ">>>>"
        print len(list(tags))
        for t in tags:
            assert_equal(['8811', '2'], t, "Tags incorrect")

    def test_read_image_hierarchy(self):
        h = dcm.read_image_hierarchy(FIXTURE)
        assert_equal(HIERARCHY, list(h))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
