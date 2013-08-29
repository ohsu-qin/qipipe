import os, re
from nose.tools import *
from nipype.interfaces.base import Undefined
from qipipe.helpers.logging_helper import logger
from qipipe.interfaces.lookup import Lookup
from test import ROOT

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'lookup')
"""The test results directory."""

class TestLookup:
    """Lookup interface unit tests."""
    
    def test_lookup(self):
        lookup = Lookup(key='a', dictionary=dict(a=1, b=2))
        result = lookup.run()
        assert_equal(1, result.outputs.value, "Output field a incorrect: %s" % result.outputs.value)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
