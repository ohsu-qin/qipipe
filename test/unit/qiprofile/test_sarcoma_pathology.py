import os
import glob
import shutil
from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_is_not_none, assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.common import TumorExtent
from qiprofile_rest_client.model.clinical import (
    Biopsy, SarcomaPathology, NecrosisPercentValue, TNM, TumorLocation,
    FNCLCCGrade
)
from qipipe.qiprofile import (xls, sarcoma_pathology)
from ...helpers.logging import logger
from . import (PROJECT, SARCOMA_FIXTURE)

SUBJECT = 1
"""Focus testing on subject 1."""

COLLECTION = 'Sarcoma'
"""The test collection."""

ROW_FIXTURE = [
    Bunch(
        subject_number=1, lesion_number=1, date=datetime(2014, 7, 3),
        intervention_type=Biopsy, weight=48, body_part='Thigh',
        sagittal_location='Left', coronal_location='Anterior',
        histology='Carcinosarcoma', length=24, width=16, depth=11,
        size=TNM.Size.parse('1'), differentiation=1, mitotic_count=2,
        lymph_status=0, metastasis=False, serum_tumor_markers=1,
        resection_boundaries=0, lymphatic_vessel_invasion=False,
        vein_invasion=0,
        necrosis_percent=NecrosisPercentValue(value=12)
    ),
    Bunch(
        subject_number=None, lesion_number=2, date=None,
        intervention_type=None, weight=None, body_part=None,
        sagittal_location=None, coronal_location=None,
        histology=None, length=17, width=11, depth=9,
        size=None, differentiation=None, mitotic_count=None,
        lymph_status=None, metastasis=None, serum_tumor_markers=None,
        resection_boundaries=None, lymphatic_vessel_invasion=None,
        vein_invasion=None, necrosis_percent=None
    )
]
"""The test rows."""


class TestSarcomaPathology(object):
    """qiprofile pathology pipeline update tests."""

    def test_read(self):
        wb = xls.load_workbook(SARCOMA_FIXTURE)
        row_iter = sarcoma_pathology.read(wb, subject_number=SUBJECT)
        rows = list(row_iter)
        assert_equal(len(rows), 4, "Sarcoma Pathology row count for Subject %s"
                                   " is incorrect: %d" % (SUBJECT, len(rows)))
        # The expected row attributes.
        expected_attrs = sorted(ROW_FIXTURE[0].keys())
        # The actual row attributes.
        row = rows[0]
        actual_attrs = sorted(row.keys())
        assert_equal(actual_attrs, expected_attrs,
                     "The row attributes are incorrect -\nexpected:\n%s"
                     "\nactual:\n%s" % (expected_attrs, actual_attrs))

    def test_update(self):
        # A test subject database object.
        subject = Subject(project=PROJECT, collection=COLLECTION,
                          number=SUBJECT)
        # Update the database object.
        sarcoma_pathology.update(subject, ROW_FIXTURE)
        # Validate the result.
        encs = subject.encounters
        assert_equal(len(encs), 1, "The encounter count is incorrect: %d" %
                                   len(encs))
        # Validate the biopsy.
        master_row = ROW_FIXTURE[0]
        biopsy = encs[0]
        assert_is_instance(biopsy, Biopsy, "Encounter type is incorrect: %s" %
                                        biopsy.__class__)
        assert_is_not_none(biopsy.date, "Biopsy is missing the date")
        assert_equal(biopsy.date, master_row.date,
                     "Biopsy date is incorrect: %s" % biopsy.date)
        assert_is_not_none(biopsy.weight, "Biopsy is missing the weight")
        assert_equal(biopsy.weight, master_row.weight,
                    "Biopsy weight is incorrect: %s" % biopsy.weight)
        # There are two lesions.
        tumor_cnt = len(biopsy.pathology.tumors)
        assert_equal(tumor_cnt, 2, "Pathology tumor count is incorrect: %d" %
                                   tumor_cnt)

        # Validate the tumor location.
        row = ROW_FIXTURE[0]
        pathology = biopsy.pathology.tumors[0]
        location = pathology.location
        assert_is_not_none(location, "The pathology is missing a tumor location")
        location_attrs = (attr for attr in TumorLocation._fields if attr in row)
        for attr in location_attrs:
            expected = getattr(row, attr)
            actual = getattr(location, attr)
            assert_equal(actual, expected, "The tumor %s is incorrect: %s" %
                                           (attr, actual))

        # Validate the tumor extent.
        for i, row in enumerate(ROW_FIXTURE):
            pathology = biopsy.pathology.tumors[i]
            extent = pathology.extent
            assert_is_not_none(extent, "The pathology is missing a tumor extent")
            extent_attrs = (attr for attr in TumorExtent._fields if attr in row)
            for attr in extent_attrs:
                expected = getattr(row, attr)
                actual = getattr(extent, attr)
                assert_equal(actual, expected, "The tumor %s is incorrect: %d" %
                                               (attr, actual))

        # Validate the TNM.
        row = ROW_FIXTURE[0]
        pathology = biopsy.pathology.tumors[0]
        tnm = pathology.tnm
        assert_is_not_none(tnm, "Pathology is missing a TNM")
        # Validate the TNM fields set from the row.
        row_tnm_attrs = (attr for attr in TNM._fields if attr in row)
        for attr in row_tnm_attrs:
            expected = getattr(row, attr)
            actual = getattr(tnm, attr)
            assert_equal(actual, expected, "TNM %s is incorrect: %s" %
                                           (attr, actual))

        # Validate the TNM grade.
        assert_is_not_none(tnm.grade, "The TNM grade is missing")
        assert_is_instance(tnm.grade, FNCLCCGrade,
                           "The TNM grade type is incorrect: %s" %
                           tnm.grade.__class__)
        grade_attrs = (attr for attr in FNCLCCGrade._fields if attr in row)
        for attr in grade_attrs:
            expected = getattr(row, attr)
            actual = getattr(tnm.grade, attr)
            assert_equal(actual, expected, "TNM grade %s is incorrect: %s" %
                                           (attr, actual))

        # Validate the full object.
        subject.validate()


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
