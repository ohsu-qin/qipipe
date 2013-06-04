from nose.tools import *
import sys, os

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.helpers.ast_config import read_config

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'helpers', 'tuning.cfg')
"""The test fixture configuration file."""


class TestASTConfig:
    """The ASTConfig unit tests."""
    def test_config(self):
        logger.debug("Testing the JSON configuration loader on %s..." % FIXTURE)
        cfg = read_config(FIXTURE)
        assert_is_not_none(cfg.has_section('Tuning'), "The configuration is missing the Tuning section")
        opts = cfg['Tuning']
        expected = dict(method = 'FFT',
            description = "String with [ ( 3 , a\" ' 4.5 'b' ) ] characters",
            iterations = [1, [2, [3, 4], 5]],
            parameters = [(1,), (2, 3)],
            sampling = [0.3, [None, [None]*2, 1.0]],
            two_tailed = [True, False],
            threshold = 4.0)
        assert_equal(expected, opts, "The configuration Tuning options are incorrect: %s" % opts)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
