import os
import shutil
from nose.tools import (assert_equal, assert_true)
from pyxnat.core.resources import (Experiment, Scan, Reconstruction,
                                   Resource, Assessor)
from qipipe.helpers import xnat_helper
from test import (project, ROOT)
from test.helpers.logging_helper import logger
from test.helpers.xnat_test_helper import generate_unique_name

FIXTURES = os.path.join(ROOT, 'fixtures', 'helpers', 'xnat')
"""The test fixture parent directory."""

SCAN_FIXTURE = os.path.join(FIXTURES, 'dummy_scan.nii.gz')
"""The scan test fixture."""

REG_FIXTURE = os.path.join(FIXTURES, 'dummy_reg.nii.gz')
"""The registration test fixture."""

RESULTS = os.path.join(ROOT, 'results', 'helpers', 'xnat')
"""The test results directory."""

SUBJECT = generate_unique_name(__name__)
"""The test subject name."""

SESSION = 'MR1'

SCAN = 1

RECONSTRUCTION = 'reco'

REGISTRATION = 'reg'

ASSESSOR = 'pk'


class TestXNATHelper(object):
    """The XNAT helper unit tests."""

    def setUp(self):
        shutil.rmtree(RESULTS, True)
        xnat_helper.delete_subjects(project(), SUBJECT)

    def tearDown(self):
        shutil.rmtree(RESULTS, True)
        xnat_helper.delete_subjects(project(), SUBJECT)
    
    def test_find_subject(self):
        with xnat_helper.connection() as xnat:
            sbj = xnat.find(project(), SUBJECT, create=True)
            assert_true(sbj.exists(), "Subject not created: %s" % SUBJECT)
            sbj = xnat.find(project(), SUBJECT)
            assert_true(sbj.exists(), "Subject not found: %s" % SUBJECT)
    
    def test_find_experiment(self):
        with xnat_helper.connection() as xnat:
            sbj = xnat.find(project(), SUBJECT, SESSION, create=True)
            assert_true(sbj.exists(),
                        "Subject %s session not created: %s" %
                        (SUBJECT, SESSION))
            sbj = xnat.find(project(), SUBJECT, SESSION, create=True)
            assert_true(sbj.exists(),
                        "Subject %s session not found: %s" %
                        (SUBJECT, SESSION))
    
    def test_find_resource(self):
        with xnat_helper.connection() as xnat:
            rsc = xnat.find(project(), SUBJECT, SESSION, resource=REGISTRATION,
                            create=True)
            assert_true(rsc.exists(),
                        "Subject %s session %s resource not created: %s" %
                        (SUBJECT, SESSION, REGISTRATION))
            assert_true(isinstance(rsc, Resource),
                        "Subject %s session %s resource %s class incorrect:"
                        " %s" % (SUBJECT, SESSION, REGISTRATION,
                                 rsc.__class__.__name__))
            rsc = xnat.find(project(), SUBJECT, SESSION, resource=REGISTRATION)
            assert_true(rsc.exists(),
                        "Subject %s session %s resource not found: %s" %
                        (SUBJECT, SESSION, REGISTRATION))
    
    def test_find_reconstruction(self):
        with xnat_helper.connection() as xnat:
            reco = xnat.find(project(), SUBJECT, SESSION,
                            reconstruction=RECONSTRUCTION, create=True)
            assert_true(reco.exists(),
                        "Subject %s session %s reconstruction not created:"
                        " %s" % (SUBJECT, SESSION, RECONSTRUCTION))
            reco = xnat.find(project(), SUBJECT, SESSION,
                            reconstruction=RECONSTRUCTION, create=True)
            assert_true(reco.exists(),
                        "Subject %s session %s reconstruction not found:"
                        " %s" % (SUBJECT, SESSION, RECONSTRUCTION))
    
    def test_find_assessor(self):
        with xnat_helper.connection() as xnat:
            anl = xnat.find(project(), SUBJECT, SESSION, assessor=ASSESSOR,
                            create=True)
            assert_true(anl.exists(),
                        "Subject %s session %s assessor not created: %s" %
                        (SUBJECT, SESSION, ASSESSOR))
            anl = xnat.find(project(), SUBJECT, SESSION, assessor=ASSESSOR,
                            create=True)
            assert_true(anl.exists(),
                        "Subject %s session %s assessor not found: %s" %
                        (SUBJECT, SESSION, ASSESSOR))
    
    def test_scan_round_trip(self):
        with xnat_helper.connection() as xnat:
            # Upload the file.
            xnat.upload(project(), SUBJECT, SESSION, SCAN_FIXTURE, scan=SCAN,
                        modality='MR')
            _, fname = os.path.split(SCAN_FIXTURE)
            exp = xnat.get_session(project(), SUBJECT, SESSION)
            assert_true(exp.exists(),
                        "XNAT %s %s %s experiment does not exist." %
                        (project(), SUBJECT, SESSION))
            scan_obj = xnat.get_scan(project(), SUBJECT, SESSION, SCAN)
            assert_true(scan_obj.exists(),
                        "XNAT %s %s %s %s scan does not exist." %
                        (project(), SUBJECT, SESSION, SCAN))
            file_obj = scan_obj.resource('NIFTI').file(fname)
            assert_true(file_obj.exists(), "File not uploaded: %s" % fname)
    
            # Download the single uploaded file.
            files = xnat.download(project(), SUBJECT, SESSION, dest=RESULTS,
                                  scan=SCAN)
            # Download all scan files.
            all_files = xnat.download(project(), SUBJECT, SESSION,
                                      dest=RESULTS, container_type='scan')
    
        # Verify the result.
        assert_equal(len(files), 1, 
                     "The download file count is incorrect: %d" % len(files))
        f = files[0]
        assert_true(os.path.exists(f), "File not downloaded: %s" % f)
        assert_equal(set(files), set(all_files),
                     "The %s %s scan %d download differs from all scans download:"
                     " %s vs %s" % (SUBJECT, SESSION, SCAN, files, all_files))
    
    def test_registration_round_trip(self):
        with xnat_helper.connection() as xnat:
            # Upload the file.
            xnat.upload(project(), SUBJECT, SESSION, REG_FIXTURE,
                        resource=REGISTRATION)
            _, fname = os.path.split(REG_FIXTURE)
            exp = xnat.get_session(project(), SUBJECT, SESSION)
            assert_true(exp.exists(),
                        "The XNAT %s %s %s experiment does not exist." %
                        (project(), SUBJECT, SESSION))
            rsc_obj = xnat.get_experiment_resource(project(), SUBJECT, SESSION,
                                        REGISTRATION)
            assert_true(rsc_obj.exists(),
                        "The XNAT %s %s %s %s resource does not exist." %
                        (project(), SUBJECT, SESSION, REGISTRATION))
            file_obj = rsc_obj.file(fname)
            assert_true(file_obj.exists(), "File not uploaded: %s" % fname)
    
            # Download the single uploaded file.
            files = xnat.download(project(), SUBJECT, SESSION, dest=RESULTS,
                                  resource=REGISTRATION)
            # Download all resource files.
            all_files = xnat.download(project(), SUBJECT, SESSION,
                                      dest=RESULTS, container_type='resource')
    
        # Verify the result.
        assert_equal(len(files), 1, 
                     "The download file count is incorrect: %d" % len(files))
        f = files[0]
        assert_true(os.path.exists(f), "File not downloaded: %s" % f)
        assert_equal(set(files), set(all_files),
                     "The %s %s scan %d download differs from all scans"
                     " download: %s vs %s" %
                     (SUBJECT, SESSION, SCAN, files, all_files))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
