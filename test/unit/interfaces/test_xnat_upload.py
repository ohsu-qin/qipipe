from nose.tools import *
import os, glob, re, shutil

import logging
logger = logging.getLogger(__name__)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.interfaces import XNATUpload
from qipipe.helpers import xnat_helper

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'xnat')
"""The test fixture parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'xnat')
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
        self.xnat = xnat_helper.facade()
        self._delete_test_subject()
        
    def tearDown(self):
        self._delete_test_subject()
    
    def test_store_image(self):
        shutil.rmtree(RESULTS, True)
        for d in glob.glob(FIXTURE + '/subject*/session*/series*'):
            logger.debug("Testing XNATUpload on %s..." % d)
            xnat.store.inputs.collection = COLLECTION
            xnat.store.inputs.series_dir = d
            xnat.store.run()
            sbj, sess_nbr, ser_nbr = re.search('.*/(Sarcoma\d{2})/session(\d{2})/series(\d{3})', d).groups()
            sess = sbj + '_Session' + sess_nbr
            # The scan label is an unpadded number.
            scan = str(int(ser_nbr))
            stack = 'series' + ser_nbr + '.nii.gz'
            f = self.xnat.interface.select('/project/QIN').subject(sbj).experiment(sess).scan(scan).resource('NIFTI').file(stack)
            assert_true(f.exists(), "Subject %s session %s scan %s stack %s not uploaded" % (sbj, sess, scan, stack))
        
    def _delete_test_subject(self):
        sbj = self.xnat.interface.select('/project/QIN/subject/Sarcoma01')
        if sbj.exists():
            sbj.delete()

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
