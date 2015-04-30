import os
import shutil
from nose.tools import assert_true
from qipipe.pipeline import staging
import qixnat
from qipipe.staging.iterator import iter_stage
from ... import (project, ROOT)
from ...helpers.logging import logger
from ...helpers.staging import subject_sources

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixture directory."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'staging')
"""The test results directory."""


class TestStagingWorkflow(object):
    """Staging workflow unit tests."""

    def setUp(self):
        shutil.rmtree(RESULTS, True)

    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_breast(self):
        self._test_collection('Breast')

    def test_sarcoma(self):
        self._test_collection('Sarcoma')

    def _test_collection(self, collection):
        """
        Run the staging workflow on the given collection and verify that
        the sessions are created in XNAT.

        :Note: This test does not verify the CTP staging area nor the
        uploaded image file content. These features should be verified
        manually.
        
        :param collection: the AIRC collection name
        """
        fixture = os.path.join(FIXTURES, collection.lower())
        logger(__name__).debug("Testing the staging workflow on %s..." %
                               fixture)

        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'staged')
        work = os.path.join(RESULTS, 'work')

        # The test subject => directory dictionary.
        sbj_dir_dict = subject_sources(collection, fixture)
        # The test subjects.
        subjects = sbj_dir_dict.keys()
        # The test source directories.
        inputs = sbj_dir_dict.values()

        with qixnat.connect() as xnat:
            # Delete any existing test subjects.
            xnat.delete_subjects(project(), *subjects)
            # Run the workflow on each session fixture.
            for scan_input in iter_stage(collection, *inputs, dest=dest):
                work_dir = os.path.join(work, 'scan', str(scan_input.scan))
                stg_wf = staging.StagingWorkflow(base_dir=work_dir)
                stg_wf.set_inputs(scan_input, dest=dest)
                stg_wf.run()
                # Verify the result.
                sess_obj = xnat.get_session(project(), scan_input.subject,
                                            scan_input.session)
                assert_true(sess_obj.exists(),
                            "The %s %s session was not created in XNAT" %
                            (scan_input.subject, scan_input.session))
                sess_dest = os.path.join(dest, scan_input.subject, scan_input.session)
                assert_true(os.path.exists(sess_dest), "The staging area"
                            " was not created: %s" % sess_dest)
                # The XNAT scan object.
                scan_obj = xnat.get_scan(project(), scan_input.subject,
                                         scan_input.session, scan_input.scan)
                assert_true(scan_obj.exists(),
                            "The %s %s scan %s was not created in XNAT" %
                            (scan_input.subject, scan_input.session, scan_input.scan))
                # The XNAT NiFTI resource object.
                rsc_obj = scan_obj.resource('NIFTI')
                assert_true(rsc_obj.exists(),
                            "The %s %s scan %s %s resource was not created in XNAT" %
                            (scan_input.subject, scan_input.session,
                             scan_input.scan, 'NIFTI'))
                for volume in scan_input.iterators.dicom.iterkeys():
                    fname = "volume%03d.nii.gz" % volume
                    file_obj = rsc_obj.file(fname)
                    assert_true(file_obj.exists(),
                                "The %s %s scan %s file %s was not created in XNAT" %
                                (scan_input.subject, scan_input.session,
                                 scan_input.scan, fname))

            # Delete the test subjects.
            xnat.delete_subjects(project(), *subjects)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
