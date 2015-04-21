import os
import glob
from nose.tools import (assert_equal, assert_not_equal)
import qixnat
from qiutil.logging import logger
from qipipe.staging.iterator import iter_stage
from ... import (ROOT, project)
from ...helpers.staging import subject_sources

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixtures parent directory."""


class TestStagingIterator(object):
    """iterator unit tests."""

    def setUp(self):
        # Delete the existing test subjects, since staging only detects
        # new visits.
        # The test subject => directory dictionary.
        sbj_dir_dict = subject_sources(collection, fixture)
        subjects = sbj_dir_dict.keys()
        with qixnat.connect() as xnat:
            xnat.delete_subjects(project(), *SUBJECTS)

    def test_breast(self):
        self._test_collection('Breast')

    def test_sarcoma(self):
        self._test_collection('Sarcoma')

    def _test_collection(self, collection):
        """
        Iterate on the given collection fixture subdirectory.

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
        
    def test_staging_iterator(self):
        dirs = glob.glob(FIXTURE + '/Subj*')
        stg = iter_stage('Sarcoma', *dirs)
        actual_sbjs = set([item[0] for item in stg])
        assert_not_equal(len(actual_sbjs), 0, "No scan images were discovered")
        assert_equal(actual_sbjs, SUBJECTS, "Subjects are incorrect: %s" %
                                                 actual_sbjs)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)

