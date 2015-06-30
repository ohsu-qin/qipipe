import os
import glob
import shutil
from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_is_not_none, assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import (
    Biopsy, SarcomaPathology, NecrosisPercentValue, TNM, FNCLCCGrade
)
from qipipe.qiprofile import (xls, sarcoma_pathology)
from ...helpers.logging import logger
from . import (PROJECT, SARCOMA_FIXTURE)

SUBJECT = 1
"""Focus testing on subject 1."""

COLLECTION = 'Sarcoma'
"""The test collection."""

ROW_FIXTURE = Bunch(
    subject_number=1, date=datetime(2014, 7, 3), intervention_type=Biopsy,
    weight=48, location='THIGH', histology='Carcinosarcoma',
    size=TNM.Size.parse('1'), differentiation=1,
    necrosis_percent=NecrosisPercentValue(value=12),
    mitotic_count=2, lymph_status=0, metastasis=False, serum_tumor_markers=1,
    resection_boundaries=0, lymphatic_vessel_invasion=False, vein_invasion=0
)
"""The test row."""


class TestSarcomaPathology(object):
    """qiprofile pathology pipeline update tests."""

    def test_read(self):
        wb = xls.load_workbook(SARCOMA_FIXTURE)
        row_iter = sarcoma_pathology.read(wb, subject_number=SUBJECT)
        rows = list(row_iter)
        assert_equal(len(rows), 2, "Sarcoma Pathology row count for subject %s"
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
        sarcoma_pathology.update(subject, rows)
        # Validate the result.
        encs = subject.encounters
        assert_equal(len(encs), 1, "The encounter count is incorrect: %d" %
                                   len(encs))
        # Validate the biopsy.
        biopsy = encs[0]
        assert_is_instance(biopsy, Biopsy, "Encounter type is incorrect: %s" %
                                        biopsy.__class__)
        assert_is_not_none(biopsy.date, "Biopsy is missing the date")
        assert_equal(biopsy.date, row.date, "Biopsy date is incorrect: %s" %
                                            biopsy.date)
        assert_is_not_none(biopsy.weight, "Biopsy is missing the weight")
        assert_equal(biopsy.weight, row.weight, "Biopsy weight is incorrect: %s" %
                                                biopsy.weight)
        # Validate the TNM.
        assert_is_not_none(biopsy.pathology, "Biopsy is missing a pathology")
        tnm = biopsy.pathology.tnm
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
