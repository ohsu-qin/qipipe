import os
import glob
import shutil
from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_is_none,
                        assert_is_not_none, assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import Treatment
from qipipe.qiprofile import treatment
from ...helpers.logging import logger
from . import (PROJECT, BREAST_FIXTURE, BREAST_SUBJECT)


class TestTreatment(object):
    """qiprofile treatment
     pipeline update tests."""

    def test_read(self):
        rows = list(treatment.filter(BREAST_FIXTURE, BREAST_SUBJECT))
        assert_equal(len(rows), 2, "Treatment row count for subject %s is"
                                   " incorrect: %d" %
                                   (BREAST_SUBJECT, len(rows)))
        for row_num, row in enumerate(rows):
            for attr in ['treatment_type', 'start_date', 'end_date']:
                assert_in(attr, row, "Treatment row %d attribute is missing: %s" %
                                     (row_num, attr))
            for attr in ['treatment_type', 'start_date']:
                assert_is_not_none(row[attr], "Treatment row %d attribute value is"
                                             " missing: %s" % (row_num, attr))
            assert_is_instance(row.start_date, datetime,
                               "Treatment row %d start date type is incorrect: %s" %
                               (row_num, row.start_date.__class__))
            if row.end_date:
                assert_is_instance(row.end_date, datetime,
                                   "Treatment row %d end date type is incorrect: %s" %
                                   (row_num, row.end_date.__class__))
            assert_equal(row.treatment_type, row.treatment_type.capitalize(),
                               "Treatment row %d treatment type is not"
                               " capitalized: %s" %
                               (row_num, row.treatment_type))

    def test_update(self):
        # A test subject database object.
        trts = [
            Treatment(treatment_type='Neoadjuvant',
                      start_date=datetime(2014, 01, 15),
                      end_date=datetime(2014, 02, 14))
        ]
        subject = Subject(project=PROJECT, treatments=trts)

        # Simulate parsed treatment rows with one update and one
        # create.
        rows = [
            Bunch(treatment_type='Neoadjuvant', start_date=datetime(2014, 01, 15),
                  end_date=datetime(2014, 02, 14)),
            Bunch(treatment_type='Adjuvant', start_date=datetime(2014, 02, 15),
                  end_date=None)
        ]

        # Update the database object.
        treatment.update(subject, rows)

        # Validate the updated subject.
        assert_equal(len(subject.treatments), 2,
                     "The treatments count is incorrect: %d" %
                     len(subject.treatments))
        for i, trt in enumerate(subject.treatments):
            self._validate(trt, rows[i])

    def _validate(self, target, row):
        for attr, expected in row.iteritems():
            actual = getattr(target, attr)
            assert_equal(actual, expected, "The treatment %s is incorrect: %s" %
                                           (attr, actual))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
