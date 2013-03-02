from nose.tools import *
import os, glob, re, shutil
import pyxnat

import logging
logger = logging.getLogger(__name__)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.pipelines import xnat
from qipipe.helpers.xnat_helper import XNAT

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'pipelines', 'xnat')
"""The test fixture parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'xnat')
"""The test results parent directory."""

COLLECTION = 'Sarcoma'
"""The test collection."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestXNAT:
    """Pipeline XNAT helper unit tests."""
    
    def setUp(self):
        self.xf = pyxnat.Interface(config=XNAT.default_configuration())
        self._delete_test_subject()
        
    def tearDown(self):
        self._delete_test_subject()
    
    def test_store_image(self):
        shutil.rmtree(RESULTS, True)
        for d in glob.glob(FIXTURE + '/patient*/visit*/series*'):
            logger.debug("Testing XNAT pipeline on %s..." % d)
            xnat.store.inputs.collection = COLLECTION
            xnat.store.inputs.series_dir = d
            xnat.store.run()
            sbj_nbr, sess_nbr, ser_nbr = re.search('.*/patient(\d{2})/visit(\d{2})/series(\d{3})', d).groups()
            sbj = COLLECTION + sbj_nbr
            sess = sbj + '_Session' + sess_nbr
            # The scan label is an unpadded number.
            scan = str(int(ser_nbr))
            stack = 'series' + ser_nbr + '.nii.gz'
            f = self.xf.select('/project/QIN').subject(sbj).experiment(sess).scan(scan).resource('NIFTI').file(stack)
            assert_true(f.exists(), "Subject %s session %s scan %s stack %s not uploaded" % (sbj, sess, scan, stack))
        
    def _delete_test_subject(self):
        sbj = self.xf.select('/project/QIN/subject/Sarcoma01')
        if sbj.exists():
            sbj.delete()

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
