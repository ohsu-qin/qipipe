import datetime
import pytz
from nose.tools import (assert_is_none)

from qipipe.helpers.qiprofile_helper import QIProfile
from qiprofile.models import Project
from test import project
from test.helpers.logging_helper import logger

SUBJECT = 'Breast099'
"""The test subject."""

SESSION = 'Session01'
"""The test session."""


class TestQIProfileHelper(object):
    """The Imaging Profile helper unit tests."""

    def setUp(self):
        if not Project.objects.filter(name=project()):
            Project(name=project()).save()
        self._db = QIProfile()
        self._clear()

    def tearDown(self):
        self._clear()
    
    def test_save_subject(self):
        self._db.save_subject(project(), SUBJECT)
    
    def test_save_session(self):
        date = datetime.datetime(2013, 7, 4, tzinfo=pytz.utc)
        self._db.save_session(project(), SUBJECT, SESSION,
                              acquisition_date=date)
        date = datetime.datetime(2013, 7, 4, tzinfo=pytz.utc)
        self._db.save_session(project(), SUBJECT, SESSION,
                              acquisition_date=date)

    def _clear(self):
        sbj = self._db.find_subject(project(), SUBJECT)
        if sbj:
            sbj.delete()


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
