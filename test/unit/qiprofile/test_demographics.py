from datetime import datetime
from bunch import Bunch
from mongoengine import (connect, ValidationError)
from nose.tools import (assert_equal, assert_in, assert_is_not_none,
                        assert_is_instance)
from qirest_client.model.subject import Subject
from qipipe.qiprofile import (xls, demographics)
from ...helpers.logging import logger
from ... import PROJECT
from . import BREAST_FIXTURE

COLLECTION = 'Breast'
"""The test collection."""

SUBJECT = 1
"""Focus testing on the first subject."""


class TestDemographics(object):
    """qiprofile demographics pipeline update tests."""
    
    def test_read(self):
        wb = xls.load_workbook(BREAST_FIXTURE)
        row_iter = demographics.read(wb, subject_number=SUBJECT)
        rows = list(row_iter)
        assert_equal(len(rows), 1, "Subject %d demographics row is incorrect:"
                                   " %d" % (SUBJECT, len(rows)))
        row = rows[0]
        for attr in ['birth_date', 'gender', 'races', 'ethnicity']:
            assert_in(attr, row, "Demographics row attribute is missing:"
                                 " %s" % attr)
            assert_is_not_none(getattr(row, attr),
                               "Demographics row attribute value is missing:"
                               " %s" % attr)
        assert_is_instance(row.birth_date, datetime,
                           "Demographics row birth date type is incorrect:"
                           " %s" % row.birth_date.__class__)
        assert_is_instance(row.races, list,
                           "Demographics row races type is incorrect: %s" %
                           row.races.__class__)
    
    def test_update(self):
        # A test subject database object.
        subject = Subject(project=PROJECT, collection=COLLECTION,
                          number=SUBJECT)
        # Simulate a parsed demographics row.
        row = Bunch(subject_number=SUBJECT,
                    birth_date=datetime(1976, 04, 15),
                    gender='Female',
                    races=['White'],
                    ethnicity='Non-Hispanic')
        # Update the database object.
        demographics.update(subject, [row])
        # Validate the updated subject.
        updated_attrs = (attr for attr in Subject._fields if attr in row)
        for attr in updated_attrs:
            expected = getattr(row, attr)
            actual = getattr(subject, attr)
            assert_equal(actual, expected, "The updated %s value is incorrect:"
                                           " %s" % (attr, actual))
        # Validate the full object.
        subject.validate()


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
