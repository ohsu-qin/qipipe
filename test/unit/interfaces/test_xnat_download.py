from nose.tools import *
import sys, os, glob, re, shutil

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.helpers.xnat_helper import PROJECT
from qipipe.interfaces import XNATDownload
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from test.helpers.xnat_test_helper import generate_subject_name

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'xnat', 'Sarcoma001', 'Session01')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'xnat')
"""The test results directory."""

SUBJECT = generate_subject_name(__name__)
"""The test subject name."""

SESSION = "%s_%s" % (SUBJECT, 'Session01')
"""The test session name."""

SCAN = 9
"""The test scan number."""

FORMAT = 'DICOM'
"""The test format."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestXNATDownload:
    """The  XNAT download interface unit tests."""
    
    def setUp(self):
        delete_subjects(PROJECT, SUBJECT)
        shutil.rmtree(RESULTS, True)
        
        with xnat_helper.connection() as xnat:
            # The XNAT test subject.
            sbj = xnat.interface.select('/project/QIN').subject(SUBJECT)
            # The test file objects.
            self._file_names = set()
            for scan_dir in glob.glob(FIXTURE + '/Series*'):
                    _, scan = os.path.split(scan_dir)
                    for f in glob.glob(scan_dir + '/*.dcm.gz'):
                        _, fname = os.path.split(f)
                        self._file_names.add(fname)
                        file_obj = sbj.experiment(SESSION).scan(str(SCAN)).resource(FORMAT).file(fname)
                        # Upload the file.
                        file_obj.insert(f, experiments='xnat:MRSessionData', format=FORMAT)
        logger.debug("Uploaded the test %s files %s." % (SESSION, list(self._file_names)))
    
    def tearDown(self):
        delete_subjects(PROJECT, SUBJECT)
        shutil.rmtree(RESULTS, True)
    
    def test_download(self):
        logger.debug("Testing the XNATDownload interface on %s..." % SUBJECT)
        # Download the files.
        download = XNATDownload(project=PROJECT, subject=SUBJECT, session=SESSION,
            scan=9, format=FORMAT, dest=RESULTS)
        result = download.run()
        
        # Verify the result
        dl_files = result.outputs.out_files
        assert_equals(2, len(dl_files), "The %s download file count is incorrect: %s" % (SESSION, dl_files))
        for f in dl_files:
            assert_true(os.path.exists(f), "The file was not downloaded: %s" % f)
            fdir, fname = os.path.split(f)
            assert_true(os.path.samefile(RESULTS, fdir), "The download location is incorrect: %s" % fdir)
            _, srcname = os.path.split(FIXTURE)
            assert_true(fname in self._file_names, "The download file name is incorrect: %s" % fname)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
