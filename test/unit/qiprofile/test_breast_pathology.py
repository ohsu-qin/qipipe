import os
import glob
import shutil
from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_is_none,
                        assert_true, assert_is_not_none, assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.common import TumorExtent
from qiprofile_rest_client.model.clinical import (
    Surgery, BreastSurgery, BreastPathology, TumorLocation, TNM,
    ModifiedBloomRichardsonGrade, ResidualCancerBurden
)
from qipipe.qiprofile import (xls, breast_pathology)
from ...helpers.logging import logger
from ... import PROJECT
from . import BREAST_FIXTURE

SUBJECT = 1
"""Focus testing on subject 1."""

COLLECTION = 'Breast'
"""The test collection."""

ROW_FIXTURE = Bunch(
    subject_number=1, lesion_number=1, date=datetime(2014, 3, 1), weight=52,
    sagittal_location='Left', intervention_type=Surgery,
    surgery_type='Partial Mastectomy', size=TNM.Size.parse('3a'),
    tubular_formation=2, nuclear_pleomorphism=1, mitotic_count=2,
    lymph_status=2, metastasis=True, serum_tumor_markers=2,
    resection_boundaries=2, lymphatic_vessel_invasion=True, vein_invasion=1,
    tumor_cell_density=20, dcis_cell_density=10, positive_node_count=3,
    total_node_count=7, largest_nodal_metastasis_length = 8,
    length=24, width=16, depth=11, estrogen_positive=True,
    estrogen_quick_score=5, estrogen_intensity=80, progesterone_positive=True,
    progesterone_quick_score=5, progesterone_intensity=80, her2_neu_ihc=2,
    her2_neu_fish=False, ki67=12
)
"""The test row."""


class TestBreastPathology(object):
    """qiprofile pathology pipeline update tests."""

    def test_read(self):
        wb = xls.load_workbook(BREAST_FIXTURE)
        row_iter = breast_pathology.read(wb, subject_number=SUBJECT)
        rows = list(row_iter)
        # There are two visits with two lesions each.
        assert_equal(len(rows), 4, "Breast Pathology row count for Subject %s"
                                   " is incorrect: %d" % (SUBJECT, len(rows)))
        # The expected row attributes.
        expected_attrs = sorted(ROW_FIXTURE.keys())
        # The actual row attributes.
        row = rows[0]
        actual_attrs = sorted(str(key) for key in row.iterkeys())
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

        # There is one lesion.
        tumor_cnt = len(surgery.pathology.tumors)
        assert_equal(tumor_cnt, 1, "Pathology tumor count is incorrect: %d" %
                                   tumor_cnt)
        pathology = surgery.pathology.tumors[0]

        # Validate the tumor location.
        location = pathology.location
        assert_is_not_none(location, "The pathology is missing a tumor location")
        location_attrs = (attr for attr in TumorLocation._fields if attr in row)
        for attr in location_attrs:
            expected = getattr(row, attr)
            actual = getattr(location, attr)
            assert_equal(actual, expected, "The tumor %s is incorrect: %s" %
                                           (attr, actual))

        # Validate the tumor extent.
        extent = pathology.extent
        assert_is_not_none(extent, "The pathology is missing a tumor extent")
        extent_attrs = (attr for attr in TumorExtent._fields if attr in row)
        for attr in extent_attrs:
            expected = getattr(row, attr)
            actual = getattr(extent, attr)
            assert_equal(actual, expected, "The tumor %s is incorrect: %d" %
                                           (attr, actual))
        
        # Validate the TNM.
        tnm = pathology.tnm
        assert_is_not_none(tnm, "The pathology is missing a TNM")
        # Validate the TNM fields set from the row.
        tnm_attrs = (attr for attr in TNM._fields if attr in row)
        for attr in tnm_attrs:
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

        # Validate the RCB.
        rcb = pathology.rcb
        assert_is_not_none(rcb, "The RCB is missing")
        rcb_attrs = (
            attr for attr in ResidualCancerBurden._fields if attr in row
        )
        for attr in rcb_attrs:
            expected = getattr(row, attr)
            actual = getattr(rcb, attr)
            assert_equal(actual, expected, "The RCB %s is incorrect: %s" %
                                           (attr, actual))
        
        # Validate the full object.
        subject.validate()


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
