import os
import glob
from nose.tools import (assert_equal, assert_not_equal)
import qixnat
from qiutil.logging import logger
from qipipe.staging.staging_helper import iter_stage
from ... import (ROOT, project)

FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'sarcoma')
"""The test fixture directory."""

SUBJECTS = set(["Sarcoma00%d" % n for n in range(1, 3)])
"""The test subjects."""


class TestStagingHelper(object):
    """staging_helper unit tests."""

    def setUp(self):
        # Delete the existing test subjects, since staging only detects
        # new visits.
        with qixnat.connect() as xnat:
            xnat.delete_subjects(project(), *SUBJECTS)
        
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

