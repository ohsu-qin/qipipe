import os
import glob
import shutil
from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_is_none,
                        assert_true, assert_is_not_none, assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import (
    Surgery, BreastSurgery, BreastPathology, TNM, ModifiedBloomRichardsonGrade
)
from qipipe.qiprofile import (xls, breast_pathology)
from ...helpers.logging import logger
from . import (PROJECT, BREAST_FIXTURE)

SUBJECT = 1
"""Focus testing on subject 1."""

COLLECTION = 'Breast'
"""The test collection."""

ROW_FIXTURE = Bunch(
    subject_number=1, date=datetime(2014, 3, 1), weight=52,
    intervention_type=Surgery, surgery_type='Partial Mastectomy',
    size=TNM.Size.parse('3a'), tubular_formation=2, nuclear_pleomorphism=1,
    mitotic_count=2, lymph_status=2, metastasis=True, serum_tumor_markers=2,
    resection_boundaries=2, lymphatic_vessel_invasion=True, vein_invasion=1,
    estrogen_positive=True, estrogen_quick_score=5, estrogen_intensity=80,
    progesterone_positive=True, progesterone_quick_score=5,
    progesterone_intensity=80, her2_neu_ihc=2, her2_neu_fish=False, ki67=12
)
"""The test row."""


class TestBreastPathology(object):
    """qiprofile pathology pipeline update tests."""

    def test_read(self):
        wb = xls.load_workbook(BREAST_FIXTURE)
        row_iter = breast_pathology.read(wb, subject_number=SUBJECT)
        rows = list(row_iter)
        assert_equal(len(rows), 2, "Breast Pathology row count for subject %s"
                                   " is incorrect: %d" % (SUBJECT, len(rows)))
        # The expected row attributes.
        expected_attrs = sorted(ROW_FIXTURE.keys())
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
        # Simulate the parsed pathology rows.
        row = ROW_FIXTURE
        rows = [row]
        # Update the database object.
        breast_pathology.update(subject, rows)
        # Validate the result.
        encs = subject.encounters
        assert_equal(len(encs), 1, "The encounter count is incorrect: %d" %
                                   len(encs))
        # Validate the surgery.
        surgery = encs[0]
        assert_is_instance(surgery, BreastSurgery,
                           "The encounter type is incorrect: %s" %
                           surgery.__class__)
        assert_is_not_none(surgery.date, "Surgery is missing the date")
        assert_equal(surgery.date, row.date,
                     "The surgery date is incorrect: %s" % surgery.date)
        assert_is_not_none(surgery.weight, "Surgery is missing the weight")
        assert_equal(surgery.weight, row.weight,
                     "The surgery weight is incorrect: %s" % surgery.weight)
        assert_is_not_none(surgery.surgery_type,
                           "The surgery is missing the surgery type")
        assert_equal(surgery.surgery_type, row.surgery_type,
                     "The surgery type is incorrect: %s" % surgery.surgery_type)
        # Validate the TNM.
        assert_is_not_none(surgery.pathology, "The surgery is missing a pathology")
        tnm = surgery.pathology.tnm
        assert_is_not_none(tnm, "The pathology is missing a TNM")
        # Validate the TNM fields set from the row.
        row_tnm_attrs = (attr for attr in TNM._fields if attr in row)
        for attr in row_tnm_attrs:
            expected = getattr(row, attr)
            actual = getattr(tnm, attr)
            assert_equal(actual, expected, "The TNM %s is incorrect: %s" %
                                           (attr, actual))
        # Validate the TNM grade.
        assert_is_not_none(tnm.grade, "The TNM grade is missing")
        assert_is_instance(tnm.grade, ModifiedBloomRichardsonGrade,
                           "The TNM grade type is incorrect: %s" %
                           tnm.grade.__class__)
        grade_attrs = (
            attr for attr in ModifiedBloomRichardsonGrade._fields if attr in row
        )
        for attr in grade_attrs:
            expected = getattr(row, attr)
            actual = getattr(tnm.grade, attr)
            assert_equal(actual, expected, "The TNM grade %s is incorrect: %s" %
                                           (attr, actual))
        # Validate the full object.
        subject.validate()


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
