import os
import glob
from nose.tools import (assert_equal, assert_true)
from qipipe.helpers import dicom_helper as dcm
from test.helpers.logging_helper import logger

FIXTURE = os.path.join(os.path.dirname(__file__),
                       '..', '..', 'fixtures', 'helpers', 'edit_dicom')
"""The test image parent directory."""

SBJ_ID = 'Sarcoma002'
"""The Subject ID."""

STUDY_ID = '1'
"""The test image Study ID."""

STUDY_UID = '1.3.12.2.1107.5.2.32.35139.30000010011316342567100000106'
"""The test image Study UID."""

SERIES_NBR = 11
"""The test image Series Number."""

SERIES_UID = '1.3.12.2.1107.5.2.32.35139.2010011914134225154552501.0.0.0'
"""The test image Series UID."""

INSTANCE_NBR = 6
"""The test image Instance Number."""


class TestDicomHelper(object):

    """dicom_helper unit tests."""

    def test_read_dicom_tags(self):
        # The first brain image.
        files = glob.glob(FIXTURE + '/*')
        # Read the tags.
        for ds in dcm.iter_dicom_headers(FIXTURE):
            tdict = dcm.select_dicom_tags(ds, 'Study ID', 'Series Number')
            study = tdict['Study ID']
            assert_equal(study, STUDY_ID, "Study tag incorrect: %s" % study)
            series = tdict['Series Number']
            assert_equal(
                series, SERIES_NBR, "Series tag incorrect: %d" % series)

    def test_read_image_hierarchy(self):
        hierarchies = list(dcm.read_image_hierarchy(FIXTURE))
        assert_true(not not hierarchies,
                    "The DICOM Helper did not detect an image hierarchy")
        assert_equal(len(hierarchies), 1,
                     "The DICOM Helper read too many image hierarchies")
        hierarchy = hierarchies[0]
        assert_equal(
            len(hierarchy), 4, "The DICOM Helper image hierarchy item count is incorrect")
        sbj_id, study_uid, series_uid, inst_nbr = hierarchy
        assert_equal(sbj_id, SBJ_ID, "Subject ID incorrect: %s" % sbj_id)
        assert_equal(study_uid, STUDY_UID,
                     "Study UID incorrect: %s" % study_uid)
        assert_equal(series_uid, SERIES_UID,
                     "Series UID incorrect: %s" % series_uid)
        assert_equal(inst_nbr, INSTANCE_NBR,
                     "Instance Number incorrect: %s" % inst_nbr)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
