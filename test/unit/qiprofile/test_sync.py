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
from . import (PROJECT, BREAST_FIXTURE, BREAST_SUBJECT, SESSION)

COLLECTION = 'Breast'
"""The test collection."""


class TestSync(object):
    """
    Database sync tests.
    """

    def setup(self):
        connect(db='qiprofile_test')
        self.db = get_db()
        self.db.connection.drop_database('qiprofile_test')

    def tearDown(self):
      self.db.connection.drop_database('qiprofile_test')

    def test_sync(self):
        # Simulate a row bunch read from a demographics Excel workbook file.
        row = Bunch(subject='Breast001',
                    birth_date=datetime(1976, 04, 15),
                    races=['White', 'Asian'],
                    ethnicity='Non-Hispanic')
        # Create the database object.
        subject = Subject(project=PROJECT, collection=COLLECTION, number=1)
        subject = demographics.update(subject, row)
        # Validate the saved subject.
        assert_equal(Subject.objects.count(), 1, "The saved subjects count is"
                                                 " incorrect: %d." %
                                                 Subject.objects.count())
        subject = Subject.objects.get(number=1)
        for attr in ['birth_date', 'races', 'ethnicity']:
            expected = getattr(row, attr)
            actual = getattr(subject, attr)
            assert_equal(actual, expected, "Saved %s value is incorrect: %s" %
                                           (attr, actual))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
