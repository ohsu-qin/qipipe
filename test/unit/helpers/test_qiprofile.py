import os
import glob
import shutil
from nose.tools import (assert_equal, assert_is_not_none)
import qixnat
from qiprofile_rest.test.helpers import seed
from ... import (ROOT, PROJECT)
from ...helpers.logging import logger
from qipipe.helpers import qiprofile

COLLECTION = 'Sarcoma'
"""The test collection."""

BREAST_SUBJECT = 'Breast001'
"""The Breast test subject."""

SARCOMA_SUBJECT = 'Sarcoma001'
"""The Sarcoma test subject."""

SESSION = 'Session01'
"""The test session."""


class TestQIProfile(object):
    """
    qiprofile update tests.
    
    :Note: the :class:`TestQIProfile` sync test cases require that the
        MongoDB and qiprofile REST servers are running on localhost.
    """

    def test_demographics_reader(self):
        
    
    # def setUp(self):
    #     self._clean()
    #     self._seed()
    # 
    # def tearDown(self):
    #     self._clean()
    # 
    # def test_sync_session(self):
    #     logger(__name__).debug("Testing qiprofile sync on %s %s..." %
    #                            (SUBJECT, SESSION))
    # 
    # def _clean(self):
    #     """Deletes the test XNAT subjects."""
    #     with qixnat.connect() as xnat:
    #         # Delete the test subject, if it exists.
    #         xnat.delete_subjects(PROJECT, subject)
    # 
    # def _seed(self):
    #     """Populates the test XNAT subjects."""
    #     with qixnat.connect() as xnat:
    #         # Delete the test subject, if it exists.
    #         xnat.delete_subjects(PROJECT, subject)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
