import os
import glob
from nose.tools import assert_equal
from qipipe.staging.staging_helper import iter_stage
from qiutil.logging import logger
from ... import ROOT

# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'sarcoma')


class TestStagingHelper(object):
    """staging_helper unit tests."""

    def test_staging_iterator(self):
        dirs = glob.glob(FIXTURE + '/Subj*')
        sbj_dict = {sbj: {sess: vol_dcm_dict}
                    for sbj, sess, vol_dcm_dict in iter_stage('Sarcoma', *dirs)}
        expected_sbjs = set(["Sarcoma00%d" % n for n in range(1, 3)])
        actual_sbjs = set(sbj_dict.keys())
        assert_equal(actual_sbjs, expected_sbjs, "Subjects are incorrect: %s" %
                                                 actual_sbjs)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)

