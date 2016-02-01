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
from qiprofile_rest_client.helpers import database
from qiprofile_rest_client.model.subject import Subject
from qipipe.helpers import metadata
from qipipe.pipeline.registration import REG_CONF_FILE
from qipipe.pipeline.modeling import (MODELING_CONF_FILE, BOLERO_CONF_SECTIONS)
from qipipe.qiprofile import imaging
from qipipe.helpers.constants import (CONF_DIR, SUBJECT_FMT, SESSION_FMT)
from ...helpers.logging import logger
from ... import (PROJECT, ROOT)
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

DUMMY = os.path.join(ROOT, 'fixtures', 'dummy.nii.gz')
'''The test fixture empty "image" file.'''

VOLUMES = os.path.join(STAGED, 'breast', 'Breast003', 'Session01',
                       'scans', '1', 'resources', 'NIFTI')
"""The test fixture 3D volume files."""

REG_RESOURCE = 'reg_ants_test'
"""The test registration resource name."""

REG_CONF_SECTIONS = ['ants.Registration']
"""The test configuration sections."""

MODELING_RESOURCE = 'pk_test'
"""The test modeling resource name."""

MODELING_OUTPUTS = ['chisq', 'guess_model.k_trans', 'guess_model.v_e',
                    'guess_model.chisq']
"""The modeling output file names without .nii.gz extension."""

AIF_SHIFT = 2.4
"""An arbitrary AIF shift parameter."""


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
        self._seed()
    
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
            exp = xnat.find_one(PROJECT, self._subject_name,
                                self._session_name)
            imaging.update(subject, exp, bolus_arrival_index=1)
    
    
    def _seed(self):
        """
        Seeds the XNAT database with the test fixture scan :const:`VOLUMES`,
        a dummy registration and the :const:`MODELING_CONF_FILE` modeling
        profile.
        """
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
            
            # The registration resource.
            reg = scan.resource(REG_RESOURCE)
            reg.create()
            # Upload the registration images.
            # The input scan files serve as the no-op
            # realigned files.
            xnat.upload(reg, *files)
            # Make the registration profile.
            with tempfile.NamedTemporaryFile() as profile:
                self._create_profile(REG_CONF_FILE,
                                     dest=profile.name,
                                     general=dict(technique='mock'))
                profile.flush()
                # Upload the registration profile.
                xnat.upload(reg, profile.name, name='registration.cfg')
            
            # The modeling resource.
            mdl = scan.resource(MODELING_RESOURCE)
            mdl.create()
            # Make the modeling result files.
            for output in MODELING_OUTPUTS:
                name = output + '.nii.gz'
                # Upload the dummy file as a modeling result.
                xnat.upload(mdl, DUMMY, name=name)
            # Make the modeling profile.
            with tempfile.NamedTemporaryFile() as profile:
                self._create_profile(MODELING_CONF_FILE,
                                     dest=profile.name,
                                     general=dict(technique='mock'),
                                     source=dict(resource=REG_RESOURCE))
                profile.flush()
                # Upload the modeling profile.
                xnat.upload(mdl, profile.name, name='modeling.cfg')
    
    def _create_profile(self, name, dest, **opts):
        cfg_file = os.path.join(CONF_DIR, name)
        cfg = read_config(cfg_file)
        cfg_dict = dict(cfg)
        
        return metadata.create_profile(cfg_dict, [], dest, **opts)
    
