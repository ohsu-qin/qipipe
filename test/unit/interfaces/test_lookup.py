import os
from nose.tools import assert_equal
from qipipe.interfaces.lookup import Lookup
from ... import ROOT

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'lookup')
"""The test results directory."""


class TestLookup(object):
    """Lookup interface unit tests."""

    def test_lookup(self):
        lookup = Lookup(key='a', dictionary=dict(a=1, b=2))
        result = lookup.run()
        assert_equal(result.outputs.value, 1, "Output field a incorrect: %s" %
                     result.outputs.value)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
