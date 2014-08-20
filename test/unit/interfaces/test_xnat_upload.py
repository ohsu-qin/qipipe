import os, glob, re, shutil
from nose.tools import (assert_equal, assert_in, assert_true)
from qipipe.interfaces import XNATUpload
from qiutil import xnat_helper
from test import project
from test.helpers.logging_helper import logger
from test.helpers.xnat_test_helper import generate_unique_name
from test.unit.helpers.test_xnat_helper import (FIXTURES, RESULTS)

SUBJECT = generate_unique_name(__name__)
"""The test subject name."""

SESSION = 'MR1'
"""The test session name."""

SCAN = 1
"""The test scan number."""

SCAN_FIXTURE = os.path.join(FIXTURES, 'dummy_scan.nii.gz')
"""The scan test fixture."""

REGISTRATION = 'reg'
"""The test registration resource name."""

REG_FIXTURE = os.path.join(FIXTURES, 'dummy_reg.nii.gz')
"""The registration test fixture."""

RECON = 'reco'
"""The test reconstruction name."""

RECON_FIXTURE = os.path.join(FIXTURES, 'dummy_reco.nii.gz')
"""The reconstruction test fixture."""

ANALYSIS = 'pk'
"""The test analysis name."""

ANALYSIS_FIXTURE = os.path.join(FIXTURES, 'dummy_analysis.csv')
"""The analysis test fixture."""


class TestXNATUpload(object):
    """The XNAT upload interface unit tests."""
    
    def setUp(self):
        xnat_helper.delete_subjects(project(), SUBJECT)
        
    def tearDown(self):
        xnat_helper.delete_subjects(project(), SUBJECT)
        shutil.rmtree(RESULTS, True)
    
    def test_upload_scan(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " scan %d..." % (SUBJECT, SESSION, SCAN))
        # Upload the file.
        upload = XNATUpload(project=project(), subject=SUBJECT, session=SESSION,
            scan=SCAN, in_files=SCAN_FIXTURE)
        result = upload.run()
        
        # Verify the result.
        xnat_files = set(result.outputs.xnat_files)
        with xnat_helper.connection() as xnat:
            scan_obj = xnat.get_scan(project(), SUBJECT, SESSION, SCAN)
            assert_true(scan_obj.exists(),
                        "Upload did not create the %s %s scan: %s" %
                        (SUBJECT, SESSION, SCAN))
            _, fname = os.path.split(SCAN_FIXTURE)
            assert_in(fname, xnat_files,
                      "The XNATUpload result does not include the %s %s scan"
                      " %d file %s" % (SUBJECT, SESSION, SCAN, fname))
            file_obj = scan_obj.resource('NIFTI').file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s scan %d file: %s" %
                        (SUBJECT, SESSION, SCAN, fname))
    
    def test_upload_registration(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " registration resource %s..." %
                               (SUBJECT, SESSION, REGISTRATION))
        # Upload the file.
        upload = XNATUpload(project=project(), subject=SUBJECT, session=SESSION,
                            resource=REGISTRATION, in_files=REG_FIXTURE)
        result = upload.run()
        
        # Verify the result.
        with xnat_helper.connection() as xnat:
            exp_obj = xnat.get_experiment(project(), SUBJECT, SESSION)
            assert_true(exp_obj.exists(),
                "Upload did not create the %s %s experiment" %
                (SUBJECT, SESSION))
            rsc_obj = exp_obj.resource(REGISTRATION)
            assert_true(rsc_obj.exists(),
                        "XNATUpload did not create the %s %s resource: %s" %
                        (SUBJECT, SESSION, REGISTRATION))
            _, fname = os.path.split(REG_FIXTURE)
            file_obj = rsc_obj.file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s %s file: %s" %
                        (SUBJECT, SESSION, REGISTRATION, fname))
     
    def test_upload_reconstruction(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " reconstruction %s..." %
                               (SUBJECT, SESSION, RECON))
        # Upload the file.
        upload = XNATUpload(project=project(), subject=SUBJECT, session=SESSION,
                            reconstruction=RECON, resource='NIFTI',
                            in_files=RECON_FIXTURE)
        result = upload.run()
        
        # Verify the result.
        with xnat_helper.connection() as xnat:
            recon_obj = xnat.get_reconstruction(project(), SUBJECT, SESSION,
                                                RECON)
            assert_true(recon_obj.exists(),
                        "Upload did not create the %s %s reconstruction: %s" %
                        (SUBJECT, SESSION, RECON))
            _, fname = os.path.split(RECON_FIXTURE)
            file_obj = recon_obj.out_resource('NIFTI').file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s file: %s" %
                        (SUBJECT, SESSION, fname))
    
    def test_upload_analysis(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " analysis %s..." % (SUBJECT, SESSION, ANALYSIS))
        # Upload the file.
        upload = XNATUpload(project=project(), subject=SUBJECT, session=SESSION,
                            resource=ANALYSIS, in_files=ANALYSIS_FIXTURE)
        result = upload.run()
        
        # Verify the result.
        with xnat_helper.connection() as xnat:
            anl_obj = xnat.get_resource(project(), SUBJECT, SESSION, ANALYSIS)
            assert_true(anl_obj.exists(),
                        "Upload did not create the %s %s analysis: %s" % 
                        (SUBJECT, SESSION, ANALYSIS))
            _, fname = os.path.split(ANALYSIS_FIXTURE)
            file_obj = anl_obj.out_resource('params').file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s file: %s" %
                        (SUBJECT, SESSION, fname))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
