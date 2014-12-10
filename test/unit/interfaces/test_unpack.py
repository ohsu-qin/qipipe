import os
import re
from nose.tools import assert_equal
from nipype.interfaces.base import Undefined
from qipipe.interfaces.unpack import Unpack
from ... import ROOT
from ...helpers.logging import logger

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'unpack')
"""The test results directory."""


class TestUnpack(object):

    """Unpack interface unit tests."""

    def test_unpack(self):
        unpack = Unpack(input_name='list', output_names=['a', 'b'])
        unpack.inputs.list = [1, 2]
        result = unpack.run()
        assert_equal(result.outputs.a, 1, "Output field a incorrect: %s" %
                     result.outputs.a)
        assert_equal(result.outputs.b, 2, "Output field b incorrect: %s" %
                     result.outputs.b)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
