from nose.tools import *
import sys, os, glob, re, shutil

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.helpers.globals import PROJECT
from qipipe.interfaces import XNATUpload
from qipipe.helpers import xnat_helper
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from test.unit.helpers.test_xnat_helper import FIXTURE, RESULTS
from test.helpers.xnat_test_helper import generate_subject_name

SUBJECT = generate_subject_name(__name__)
"""The test subject name."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestXNATUpload:
    """The XNAT upload interface unit tests."""
    
    def setUp(self):
        delete_subjects(PROJECT, SUBJECT)
        
    def tearDown(self):
        delete_subjects(PROJECT, SUBJECT)
        shutil.rmtree(RESULTS, True)
    
    def test_upload(self):
        logger.debug("Testing the XNATUpload interface on %s..." % SUBJECT)
        # The XNAT experiment name.
        sess = "%s_%s" % (SUBJECT, 'MR1')
        # Upload the file.
        upload = XNATUpload(project=PROJECT, subject=SUBJECT, session=sess, scan=1, in_files=FIXTURE)
        result = upload.run()
        
        # Verify the result.
        with xnat_helper.connection() as xnat:
            sbj = xnat.interface.select('/project/QIN').subject(SUBJECT)
            assert_true(sbj.exists(), "Upload did not create the subject: %s" % SUBJECT)
            _, fname = os.path.split(FIXTURE)
            file_obj = sbj.experiment(sess).scan('1').resource('NIFTI').file(fname)
            assert_true(file_obj.exists(), "Upload did not upload the %s file: %s" % (SUBJECT, fname))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
