import os
import glob
import re
import shutil
from nose.tools import (assert_equal, assert_is_not_none, assert_true)
from qipipe.interfaces import XNATDownload
import qixnat
from ... import (ROOT, PROJECT)
from test.helpers.logging import logger
from test.helpers.name_generator import generate_unique_name

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'xnat', 'Sarcoma001',
                      'Session01', 'scans', '1', 'resources', 'NIFTI',
                      'volume001.nii.gz')
"""The test fixture file."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'xnat')
"""The test results directory."""

SUBJECT = generate_unique_name(__name__)
"""The test subject name."""

SESSION = 'Session01'
"""The test session name."""

SCAN = 9
"""The test scan number."""

RESOURCE = 'NIFTI'


class TestXNATDownload(object):
    """The  XNAT download interface unit tests."""

    def setUp(self):
        with qixnat.connect() as xnat:
            xnat.delete_subjects(PROJECT, SUBJECT)
        shutil.rmtree(RESULTS, True)

        with qixnat.connect() as xnat:
            # Make the XNAT scan object.
            scan_obj = xnat.get_scan(PROJECT, SUBJECT, SESSION, SCAN)
            rsc_obj = scan_obj.resource(RESOURCE)
            _, fname = os.path.split(FIXTURE)
            file_obj = rsc_obj.file(fname)
            # Upload the NiFTI file.
            file_obj.insert(FIXTURE)
        logger(__name__).debug("Uploaded the test %s %s scan %d file %s." %
                               (SUBJECT, SESSION, SCAN, FIXTURE))


    def tearDown(self):
        with qixnat.connect() as xnat:
            xnat.delete_subjects(PROJECT, SUBJECT)
        shutil.rmtree(RESULTS, True)

    def test_download_scan(self):
        logger(__name__).debug("Testing the XNATDownload interface on "
                               "%s %s scan %d..." % (SUBJECT, SESSION, SCAN))
        # Download the files.
        download = XNATDownload(project=PROJECT, subject=SUBJECT,
                                session=SESSION, scan=SCAN, resource=RESOURCE,
                                dest=RESULTS)
        result = download.run()

        # Verify the result.
        dl_file = result.outputs.out_file
        assert_is_not_none(dl_file, "The %s %s scan %s download result out_file"
                                    " property is not set" % (SUBJECT, SESSION, SCAN))
        assert_true(os.path.exists(dl_file), "The %s %s scan %s file was not"
                                             " downloaded" % (SUBJECT, SESSION, SCAN))
        fdir, fname = os.path.split(dl_file)
        assert_true(os.path.samefile(RESULTS, fdir),
                    "The download location is incorrect: %s" % fdir)
        _, srcname = os.path.split(FIXTURE)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
