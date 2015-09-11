import os
import glob
from datetime import datetime
from mongoengine import connect
from nose.tools import assert_equal
import qixnat
from qiprofile_rest_client.model.subject import Subject
from qipipe.qiprofile import imaging
from qipipe.helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from ...helpers.logging import logger
from ... import PROJECT
from ..pipeline.volume_test_base import FIXTURES as STAGED
from . import (DATABASE, FIXTURES)

COLLECTION = 'Breast'
"""The test collection."""

SUBJECT = 3
"""The test subject number."""

SESSION = 1
"""The test session number."""

SCAN = 1
"""The test scan number."""

VOLUMES = os.path.join(STAGED, 'breast', 'Breast003', 'Session01',
                       'scans', '1', 'resources', 'NIFTI')
"""The 3D volume files."""

REGISTRATION = 'reg_ants_test'
"""The test registration resource name."""

MODELING = 'pk_test'
"""The test modeling resource name."""

MODELING_FIXTURE = os.path.join(FIXTURES, 'modeling' 'parameters.csv')


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
            scan = xnat.find_or_create(PROJECT, self._subject_name,
                                       self._session_name, scan=SCAN,
                                       modality='MR')
            # Upload the 3D scan volumes.
            nifti = scan.resource('NIFTI')
            nifti.create()
            files = glob.glob(VOLUMES + '/*.nii.gz')
            xnat.upload(nifti, *files)
            # Upload the 3D no-op registration volumes.
            reg = scan.resource(REGISTRATION)
            reg.create()
            xnat.upload(reg, *files)
            # Upload a modeling result.
            mdl = scan.resource(MODELING)
            mdl.create()
            xnat.upload(mdl, MODELING_FIXTURE)

    def tearDown(self):
        self._connection.drop_database(DATABASE)
        with qixnat.connect() as xnat:
            xnat.delete(PROJECT, self._subject_name, self._session_name)

    def test_sync(self):
        # The test qiprofile subject.
        subject = Subject(project=PROJECT, collection=COLLECTION,
                          number=SUBJECT)
        with qixnat.connect() as xnat:
            # The test XNAT scan.
            scan = xnat.find(PROJECT, self._subject_name, self._session_name,
                             scan=SCAN)
            imaging.sync(SUBJECT, scan)
