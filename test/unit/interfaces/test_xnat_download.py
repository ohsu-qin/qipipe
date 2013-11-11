import os
import glob
import re
import shutil
from nose.tools import (assert_equal, assert_true)
from test.helpers.logging_helper import logger
from qipipe.interfaces import XNATDownload
from qipipe.helpers import xnat_helper
from test import (project, ROOT)
from test.helpers.xnat_test_helper import generate_subject_name

FIXTURE = os.path.join(
    ROOT, 'fixtures', 'interfaces', 'xnat', 'Sarcoma001', 'Session01')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'xnat')
"""The test results directory."""

SUBJECT = generate_subject_name(__name__)
"""The test subject name."""

SESSION = 'Session01'
"""The test session name."""

SCAN = 9
"""The test scan number."""

RESOURCE = 'DICOM'
"""The test format."""


class TestXNATDownload(object):

    """The  XNAT download interface unit tests."""

    def setUp(self):
        xnat_helper.delete_subjects(project(), SUBJECT)
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
                        scan_obj = xnat.get_scan(
                            project(), SUBJECT, SESSION, SCAN)
                        file_obj = scan_obj.resource(RESOURCE).file(fname)
                        # Upload the file.
                        file_obj.insert(f, experiments='xnat:MRSessionData',
                                        resource=RESOURCE)
        logger(__name__).debug("Uploaded the test %s %s files %s." %
              (SUBJECT, SESSION, list(self._file_names)))

    def tearDown(self):
        xnat_helper.delete_subjects(project(), SUBJECT)
        shutil.rmtree(RESULTS, True)

    def test_download_scan(self):
        logger(__name__).debug("Testing the XNATDownload interface on "
                               "%s %s scan %d..." % (SUBJECT, SESSION, SCAN))
        # Download the files.
        download = XNATDownload(project=project(), subject=SUBJECT,
                                session=SESSION, scan=9, resource=RESOURCE,
                                dest=RESULTS)
        result = download.run()

        # Verify the result
        dl_files = result.outputs.out_files
        assert_equal(len(dl_files), 2,
                     "The %s download file count is incorrect: %s" %
                            (SESSION, dl_files))
        for f in dl_files:
            assert_true(os.path.exists(f),
                        "The file was not downloaded: %s" % f)
            fdir, fname = os.path.split(f)
            assert_true(os.path.samefile(RESULTS, fdir),
                        "The download location is incorrect: %s" % fdir)
            _, srcname = os.path.split(FIXTURE)
            assert_true(fname in self._file_names,
                        "The download file name is incorrect: %s" % fname)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
