import os
import shutil
from nose.tools import (assert_equal, assert_is_not_none, assert_in,
                        assert_true)
from qipipe.interfaces import XNATUpload
import qixnat
from ... import (ROOT, PROJECT)
from ...helpers.logging import logger
from ...helpers.name_generator import generate_unique_name

SUBJECT = generate_unique_name(__name__)
"""The test subject name."""

SESSION = 'Session01'
"""The test session name."""

SCAN = 9
"""The test scan number."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'xnat', 'Sarcoma001',
                      'Session01', 'scans', '1', 'resources', 'NIFTI',
                      'volume001.nii.gz')
"""The test fixture file."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'xnat')
"""The test results directory."""

REGISTRATION = 'reg'
"""The test registration resource name."""

RECON = 'reco'
"""The test reconstruction name."""

ANALYSIS = 'pk'
"""The test analysis name."""


class TestXNATUpload(object):
    """The XNAT upload interface unit tests."""
    
    def setUp(self):
        with qixnat.connect() as xnat:
            xnat.delete(PROJECT, SUBJECT)
    
    def tearDown(self):
        with qixnat.connect() as xnat:
            xnat.delete(PROJECT, SUBJECT)
        shutil.rmtree(RESULTS, True)
    
    def test_upload_scan(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " scan %d..." % (SUBJECT, SESSION, SCAN))
        # Upload the file.
        upload = XNATUpload(project=PROJECT, subject=SUBJECT, session=SESSION,
                            scan=SCAN, resource='NIFTI', modality='MR',
                            in_files=FIXTURE)
        result = upload.run()
        
        # Verify the result.
        xnat_files = set(result.outputs.xnat_files)
        with qixnat.connect() as xnat:
            scan = xnat.find_one(PROJECT, SUBJECT, SESSION, scan=SCAN)
            assert_is_not_none(scan.exists(),
                        "Upload did not create the %s %s scan: %s" %
                        (SUBJECT, SESSION, SCAN))
            _, fname = os.path.split(FIXTURE)
            assert_in(fname, xnat_files,
                      "The XNATUpload result does not include the %s %s scan"
                      " %d file %s" % (SUBJECT, SESSION, SCAN, fname))
            file_obj = scan.resource('NIFTI').file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s scan %d file:"
                        " %s" % (SUBJECT, SESSION, SCAN, fname))
    
    def test_upload_registration(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " registration resource %s..." %
                               (SUBJECT, SESSION, REGISTRATION))
        # Upload the file.
        upload = XNATUpload(project=PROJECT, subject=SUBJECT, session=SESSION,
                            resource=REGISTRATION, modality='MR',
                            in_files=FIXTURE)
        result = upload.run()
        
        # Verify the result.
        with qixnat.connect() as xnat:
            exp_obj = xnat.find_one(PROJECT, SUBJECT, SESSION)
            assert_true(exp_obj.exists(),
                        "Upload did not create the %s %s experiment" %
                        (SUBJECT, SESSION))
            rsc_obj = exp_obj.resource(REGISTRATION)
            assert_true(rsc_obj.exists(),
                        "XNATUpload did not create the %s %s resource: %s" %
                        (SUBJECT, SESSION, REGISTRATION))
            _, fname = os.path.split(FIXTURE)
            file_obj = rsc_obj.file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s %s file: %s" %
                        (SUBJECT, SESSION, REGISTRATION, fname))
    
    def test_upload_reconstruction(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " reconstruction %s..." %
                               (SUBJECT, SESSION, RECON))
        # Upload the file.
        upload = XNATUpload(project=PROJECT, subject=SUBJECT, session=SESSION,
                            reconstruction=RECON, resource='NIFTI', modality='MR',
                            inout='out', in_files=FIXTURE)
        result = upload.run()
        
        # Verify the result.
        with qixnat.connect() as xnat:
            recon_obj = xnat.find_one(PROJECT, SUBJECT, SESSION,
                                      reconstruction=RECON)
            assert_true(recon_obj.exists(),
                        "Upload did not create the %s %s reconstruction: %s" %
                        (SUBJECT, SESSION, RECON))
            _, fname = os.path.split(FIXTURE)
            file_obj = recon_obj.out_resource('NIFTI').file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s %s file: %s" %
                        (SUBJECT, SESSION, RECON, fname))
    
    def test_upload_analysis(self):
        logger(__name__).debug("Testing the XNATUpload interface on %s %s"
                               " analysis %s..." % (SUBJECT, SESSION, ANALYSIS))
        # Upload the file.
        upload = XNATUpload(project=PROJECT, subject=SUBJECT, session=SESSION,
                            assessor=ANALYSIS, resource='params', modality='MR',
                            inout='out', in_files=FIXTURE)
        result = upload.run()
        
        # Verify the result.
        with qixnat.connect() as xnat:
            exp_obj = xnat.find_one(PROJECT, SUBJECT, SESSION)
            assert_true(exp_obj.exists(),
                        "XNATUpload did not create the %s %s experiment" %
                        (SUBJECT, SESSION))
            anl_obj = xnat.find_one(PROJECT, SUBJECT, SESSION, assessor=ANALYSIS)
            assert_true(anl_obj.exists(),
                        "XNATUpload did not create the %s %s analysis: %s" %
                        (SUBJECT, SESSION, ANALYSIS))
            _, fname = os.path.split(FIXTURE)
            file_obj = anl_obj.out_resource('params').file(fname)
            assert_true(file_obj.exists(),
                        "XNATUpload did not create the %s %s %s file: %s" %
                        (SUBJECT, SESSION, ANALYSIS, fname))


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
