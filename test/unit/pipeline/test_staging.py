import os
import shutil
from glob import glob
from nose.tools import (assert_true, assert_is_not_none)
from qipipe.pipeline import staging
from qiutil.collections import concat
import qixnat
from qipipe.staging.iterator import iter_stage
from ... import (ROOT, PROJECT, CONF_DIR)
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
        Run the staging workflow on the given collection and verify
        that the sessions are created in XNAT.

        .. Note:: This test does not verify the CTP staging area nor
            the uploaded image file content. These features should be
            verified manually.

        :param collection: the image collection name
        """
        fixture = os.path.join(FIXTURES, collection.lower())
        logger(__name__).debug("Testing the staging workflow on %s..." %
                               fixture)

        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'staged')
        work = os.path.join(RESULTS, 'work')

        # The test {subject: directory} dictionary.
        sbj_dir_dict = subject_sources(collection, fixture)
        # The test subjects.
        subjects = set(sbj_dir_dict.keys())
        # The test input subject directories.
        sbj_dirs = sbj_dir_dict.values()
        # The test input session directories.
        sess_dir_lists = (glob(d + '/*') for d in sbj_dirs)
        inputs = concat(*sess_dir_lists)

        with qixnat.connect() as xnat:
            # Delete any existing test subjects.
            for sbj in subjects:
                xnat.delete(PROJECT, sbj)
            # Run the workflow on each session fixture.
            for scan_input in iter_stage(PROJECT, collection, *inputs, dest=dest):
                work_dir = os.path.join(work, 'scan', str(scan_input.scan))
                stg_wf = staging.StagingWorkflow(project=PROJECT,
                                                 base_dir=work_dir,
                                                 config_dir=CONF_DIR)
                stg_wf.set_inputs(scan_input, dest=dest)
                stg_wf.run()
                # Verify the result.
                sess_obj = xnat.find_one(PROJECT, scan_input.subject,
                                            scan_input.session)
                assert_is_not_none(sess_obj.exists(),
                                   "The %s %s session was not created in XNAT" %
                                   (scan_input.subject, scan_input.session))
                sess_dest = os.path.join(dest, scan_input.subject, scan_input.session)
                assert_true(os.path.exists(sess_dest), "The staging area"
                            " was not created: %s" % sess_dest)
                # The XNAT scan object.
                scan_obj = xnat.find_one(PROJECT, scan_input.subject,
                                         scan_input.session, scan=scan_input.scan)
                assert_is_not_none(scan_obj,
                                   "The %s %s scan %s was not created in XNAT" %
                                   (scan_input.subject, scan_input.session, scan_input.scan))
                # The XNAT NIfTI resource object.
                rsc_obj = scan_obj.resource('NIFTI')
                assert_true(rsc_obj.exists(),
                            "The %s %s scan %s %s resource was not created in XNAT" %
                            (scan_input.subject, scan_input.session,
                             scan_input.scan, 'NIFTI'))
                for volume in scan_input.iterators.dicom.iterkeys():
                    base_name = "volume%03d.nii.gz" % volume
                    file_obj = rsc_obj.file(base_name)
                    assert_true(file_obj.exists(),
                                "The %s %s scan %s file %s was not created in XNAT" %
                                (scan_input.subject, scan_input.session,
                                 scan_input.scan, base_name))

            # Delete the test subjects.
            for sbj in subjects:
                xnat.delete(PROJECT, sbj)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
