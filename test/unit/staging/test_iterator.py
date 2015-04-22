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

    def test_breast(self):
        discovered = self._test_collection('Breast')
        # The first visit has both a T1 and a T2 scan.
        for visit in [1, 2]:
            session = "Session0%d" % visit
            scans = {item[2] for item in discovered if item[1] == session}
            expected_scans = set([1, 2]) if visit == 1 else set([1])
            assert_equal(scans, expected_scans,
                         "Discovered Breast003 %s scans are incorrect: %s" %
                         (session, scans))

    def test_sarcoma(self):
        self._test_collection('Sarcoma')

    def _test_collection(self, collection):
        """
        Iterate on the given collection fixture subdirectory.
        
        :param collection: the AIRC collection name
        """
        fixture = os.path.join(FIXTURES, collection.lower())
        logger(__name__).debug("Testing the staging iterator on %s..." %
                               fixture)

        # The test {subject: directory} dictionary.
        sbj_dir_dict = subject_sources(collection, fixture)
        # The test subjects.
        subjects = set(sbj_dir_dict.keys())
        # The test source directories.
        inputs = sbj_dir_dict.values()
        # Delete the existing test subjects, since staging only detects
        # new visits.
        with qixnat.connect() as xnat:
            xnat.delete_subjects(project(), *subjects)
        
        
        # Iterate over the scans.
        discovered = list(iter_stage(collection, *inputs))
        discovered_sbjs = set([item[0] for item in discovered])
        assert_not_equal(len(discovered_sbjs), 0, 'No scan images were discovered')
        assert_equal(discovered_sbjs, subjects, "Subjects are incorrect: %s" %
                                           discovered_sbjs)
        
        # Return the iterated tuples for further testing.
        return discovered    

if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)

