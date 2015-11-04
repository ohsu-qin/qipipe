import os
import glob
import tempfile
import csv
import shutil
from datetime import datetime
from mongoengine import connect
from nose.tools import assert_equal
from qiutil.ast_config import read_config
import qixnat
from qiprofile_rest_client.model.subject import Subject
from qipipe import CONF_DIR
from qipipe.pipeline.modeling import (
    FASTFIT_PARAMS_FILE, FASTFIT_PARAMS_TEMPLATE, MODELING_PROFILE_FILE,
    create_profile
)
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

OUTPUTS = ['chisq', 'guess_model.k_trans', 'guess_model.v_e',
           'guess_model.chisq']
"""The modeling output file names without .nii.gz extension."""


class TestImaging(object):
    """
    Imaging update tests.
    """

    def setup(self):
        self._connection = connect(db=DATABASE)
        self._connection.drop_database(DATABASE)
        # Seed the XNAT test subject.
        self._subject_name = SUBJECT_FMT % (COLLECTION, SUBJECT)
        self._session_name = SESSION_FMT % SESSION
        exp_opt = (self._session_name, dict(date=datetime.now()))
        with qixnat.connect() as xnat:
            xnat.delete(PROJECT, self._subject_name, self._session_name)
            scan = xnat.find_or_create(PROJECT, self._subject_name,
                                       experiment=exp_opt, scan=SCAN,
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

            # Make a modeling resource.
            mdl = scan.resource(MODELING)
            mdl.create()

            # The static input parameter CSV template.
            with open(FASTFIT_PARAMS_TEMPLATE) as csv_file:
                rows = list(csv.reader(csv_file))
            # Add a plausible shift.
            rows.append(['aif_shift', 58.0])
            # The resource files staging area.
            dest = tempfile.mkdtemp()
            # Make the fastfit params file.
            with tempfile.NamedTemporaryFile() as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(rows)
                # Upload the file.
                xnat.upload(mdl, csv_file.name, name=FASTFIT_PARAMS_FILE)
            # Make the R1 params file.
            with tempfile.NamedTemporaryFile() as profile_dest:
                create_profile(profile_dest.name)
                xnat.upload(mdl, profile_dest.name, name=MODELING_PROFILE_FILE)
            # Make the modeling result files.
            with tempfile.NamedTemporaryFile() as dummy:
                for output in OUTPUTS:
                    name = output + '.nii.gz'
                    # Upload the dummy file.
                    xnat.upload(mdl, dummy.name, name=name)
            shutil.rmtree(dest)

    def tearDown(self):
        self._connection.drop_database(DATABASE)
        with qixnat.connect() as xnat:
            xnat.delete(PROJECT, self._subject_name, self._session_name)

    def test_update(self):
        # The test qiprofile subject.
        subject = Subject(project=PROJECT, collection=COLLECTION,
                          number=SUBJECT)
        with qixnat.connect() as xnat:
            # The test XNAT scan.
            scan = xnat.find_one(PROJECT, self._subject_name, self._session_name,
                                 scan=SCAN)
            imaging.update(subject, scan)
