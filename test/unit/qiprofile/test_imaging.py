from datetime import datetime
from mongoengine import connect
from nose.tools import assert_equal
import qixnat
from qiprofile_rest_client.model.subject import Subject
from qipipe.qiprofile import imaging
from qipipe.helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from ...helpers.logging import logger
from ... import PROJECT
from . import DATABASE

COLLECTION = 'Breast'
"""The test collection."""

SUBJECT = 1
"""The test subject number."""

SESSION = 1
"""The test session number."""

SCAN = 1
"""The test scan number."""

MODELING = 'pk_test'
"""The test modeling resource name."""


class TestImaging(object):
    """
    Imaging sync tests.
    """

    def setup(self):
        self._connection = connect(db=DATABASE)
        self._connection.drop_database(DATABASE)
        # Seed the XNAT test subject.
        self._subject_name = SUBJECT_FMT % (COLLECTION, SUBJECT)
        self._session_name = SESSION_FMT % SESSION
        with qixnat.connect() as xnat:
            xnat.delete(PROJECT, self._subject_name, self._session_name)
            sess = xnat.find_or_create(PROJECT, self._subject_name,
                                       self._session_name, modality='MR')
            self._seed_xnat(sess)

    def tearDown(self):
        self._connection.drop_database(DATABASE)
        with qixnat.connect() as xnat:
            xnat.delete(PROJECT, self._subject_name, self._session_name)

    def test_sync(self):
        # TODO
        pass

    def _seed_xnat(self, session):
        """Seeds the given XNAT test session object."""
        sess.scan(SCAN)
