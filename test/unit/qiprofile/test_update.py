from datetime import datetime
from bunch import Bunch
from mongoengine import connect
from nose.tools import assert_equal
from qirest_client.model.subject import Subject
from qipipe.qiprofile import demographics
from ...helpers.logging import logger
from ... import PROJECT
from . import (DATABASE, BREAST_FIXTURE, SARCOMA_FIXTURE)

COLLECTION = 'Breast'
"""The test collection."""

SUBJECT = 1
"""The test subject number."""

SESSION = 1
"""The test session number."""


class TestUpdate(object):
    """
    Database update tests.
    """
    
    def setup(self):
        self._connection = connect(db=DATABASE)
        self._connection.drop_database(DATABASE)
    
    def tearDown(self):
      self._connection.drop_database(DATABASE)
    
    def test_breast(self):
        # TODO - make a simpler mash-up of the clinical and imaging test cases.
        pass
    
    def test_sarcoma(self):
        # TODO - make a simpler mash-up of the clinical and imaging test cases.
        pass


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
