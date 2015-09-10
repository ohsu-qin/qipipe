from datetime import datetime
from bunch import Bunch
from mongoengine import connect
from nose.tools import assert_equal
from qiprofile_rest_client.model.subject import Subject
from qipipe.qiprofile import demographics
from ...helpers.logging import logger
from ... import PROJECT
from . import (BREAST_FIXTURE, SARCOMA_FIXTURE)

COLLECTION = 'Breast'
"""The test collection."""

SUBJECT = 1
"""The test subject number."""

SESSION = 1
"""The test session number."""


class TestSync(object):
    """
    Database sync tests.
    """

    def setup(self):
        self._connection = connect(db='qiprofile_test')
        self._connection.drop_database('qiprofile_test')

    def tearDown(self):
      self._connection.drop_database('qiprofile_test')

    def test_breast(self):
        # TODO - make a simpler mash-up of the clinical and imaging test cases.
        pass
    
    def test_sarcoma(self):
        # TODO - make a simpler mash-up of the clinical and imaging test cases.
        pass


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
