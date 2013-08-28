import os, sys, re
from nose.tools import *
from nipype.interfaces.base import Undefined

from qipipe.helpers.logging_helper import logger


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.interfaces.lookup import Lookup

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'lookup')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestLookup:
    """Lookup interface unit tests."""
    
    def test_lookup(self):
        lookup = Lookup(key='a', dictionary=dict(a=1, b=2))
        result = lookup.run()
        assert_equal(1, result.outputs.value, "Output field a incorrect: %s" % result.outputs.value)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
