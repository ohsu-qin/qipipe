from nose.tools import *
import os, re, glob, shutil

import logging
logger = logging.getLogger(__name__)

# The test parent directory.
ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'pipelines', 'stage')
# The test results parent directory.
RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'stage')
# The test results group directory.
AIRC = os.path.join(RESULTS, 'airc')
# The test results assembly directory.
CTP = os.path.join(RESULTS, 'ctp')
# The test collection.
COLLECTION = 'Sarcoma'

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

from nipype.caching import Memory

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.interfaces import StageCTP
from qipipe.helpers.dicom_helper import iter_dicom


class TestStageCTP:
    """Stage CTP pipeline unit tests."""
    
    def test_stage(self):
        shutil.rmtree(RESULTS, True)
        os.makedirs(RESULTS)
        logger.debug("Testing stage pipeline on %s..." % FIXTURE)
        subj_dirs = glob.glob(FIXTURE + '/Subj*')
        mem = Memory(RESULTS)
        staged = mem.cache(StageCTP)(collection=COLLECTION, RESULTS, *subj_dirs)
        self._verify_result(staged.outputs.series_dirs)
        # Cleanup.
        shutil.rmtree(RESULTS, True)

    def _verify_result(self, staged):
        in_files = glob.glob(FIXTURE + '/Subj*/Visit*/*/*')
        assert_equal(len(in_files), len(staged),
            "Staged file count incorrect - expected %d, found %d" % (len(in_files), len(staged)))
        for d in glob.glob(FIXTURE + '/Subj*'):
            _verify_subject_result(self, sbj_dir)

    def _verify_subject_result(self, sbj_dir):
        basename = 'subject0' + sbj_dir[-1]
        ctp_dir = os.path.join(CTP, basename)
        assert_true(os.path.exists(ctp_dir), "Result not found: %s" % ctp_dir)
        for ds in iter_dicom(ctp_dir):
            pt_id = 'Sarcoma0' + sbj_dir[-1]
            assert_equal(pt_id, ds.PatientID, "Incorrect Patient ID: %s" % ds.PatientID)
            assert_is_not_none(ds.BodyPartExamined, "Incorrect Body Part: %s" % ds.BodyPartExamined)
        prop_files = glob.glob(os.path.join(CTP, '*.properties'))
        assert_equal(1, len(prop_files), "Unique mapping file not found in %s" % CTP)
        pat = 'ptid/' + pt_id
        assert_true(any([l.startswith(pat) for l in open(prop_files[0])]))

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
