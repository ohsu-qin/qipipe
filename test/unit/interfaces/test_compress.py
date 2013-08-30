import os, shutil
from nose.tools import (assert_equal, assert_true)
from qipipe.interfaces.compress import Compress
from test import ROOT

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'compress', 'small.txt')
"""The test fixture file."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'compress')
"""The test results directory."""

class TestCompress:
    """Compress interface unit tests."""
    
    def test_compress(self):
        shutil.rmtree(RESULTS, True)
        compress = Compress(in_file=FIXTURE, dest=RESULTS)
        target = os.path.join(RESULTS, 'small.txt.gz')
        result = compress.run()
        assert_equal(result.outputs.out_file, target, "Compress output file"
            " name incorrect: %s" % result.outputs.out_file)
        assert_true(os.path.exists(target))
        shutil.rmtree(RESULTS, True)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
