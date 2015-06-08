from datetime import datetime
from bunch import Bunch
from mongoengine import (connect, ValidationError)
from mongoengine.connection import get_db
from nose.tools import (assert_true, assert_equal, assert_in,
                        assert_is_none, assert_is_not_none,
                        assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qipipe.qiprofile import demographics
from ...helpers.logging import logger
from . import (PROJECT, BREAST_FIXTURE, BREAST_SUBJECT)

COLLECTION = 'Breast'
"""The test collection."""


class TestDemographics(object):
    """qiprofile demographics pipeline update tests."""

    def setup(self):
        connect(db='qiprofile_test')
        self.db = get_db()
        self.db.connection.drop_database('qiprofile_test')
    
    def tearDown(self):
      self.db.connection.drop_database('qiprofile_test')

    def test_read(self):
        row = demographics.filter(BREAST_FIXTURE, BREAST_SUBJECT)
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
        subject = Subject(project=PROJECT)
        # Simulate a parsed demographics row.
        row = Bunch(subject='Breast001',
                    birth_date=datetime(1976, 04, 15),
                    races=['White', 'Asian'],
                    ethnicity='Non-Hispanic')
        # Update the database object.
        demographics.update(subject, row)
        # Validate the updated subject.
        for attr in ['birth_date', 'races', 'ethnicity']:
            expected = getattr(row, attr)
            actual = getattr(subject, attr)
            assert_equal(actual, expected, "The updated %s value is incorrect:"
                                           " %s" % (attr, actual))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
