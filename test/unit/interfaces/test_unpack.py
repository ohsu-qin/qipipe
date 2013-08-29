import os, sys, re
from nose.tools import *
from nipype.interfaces.base import Undefined
from qipipe.helpers.logging_helper import logger


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.interfaces.unpack import Unpack

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'unpack')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestUnpack:
    """Unpack interface unit tests."""
    
    def test_unpack(self):
        unpack = Unpack(input_name='list', output_names=['a', 'b'])
        unpack.inputs.list = [1, 2]
        result = unpack.run()
        assert_equal(1, result.outputs.a, "Output field a incorrect: %s" % result.outputs.a)
        assert_equal(2, result.outputs.b, "Output field b incorrect: %s" % result.outputs.b)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
