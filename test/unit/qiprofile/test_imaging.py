from datetime import datetime
from mongoengine import connect
from nose.tools import assert_equal
from qiprofile_rest_client.model.subject import Subject
from qipipe.qiprofile import imaging
from ...helpers.logging import logger
from ... import PROJECT

COLLECTION = 'Breast'
"""The test collection."""

SUBJECT = 1
"""The test subject number."""

SESSION = 1
"""The test session number."""


class TestImaging(object):
    """
    Imaging sync tests.
    """

    def setup(self):
        self._connection = connect(db='qiprofile_test')
        self._connection.drop_database('qiprofile_test')

    def tearDown(self):
      self._connection.drop_database('qiprofile_test')

    def test_sync(self):
        # TODO
        pass

