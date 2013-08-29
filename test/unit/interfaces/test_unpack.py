import os, sys, re
from nose.tools import *
from nipype.interfaces.base import Undefined
from qipipe.interfaces.unpack import Unpack
from test import ROOT

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'unpack')
"""The test results directory."""


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
