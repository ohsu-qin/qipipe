import os, sys, shutil
from nose.tools import *

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.interfaces.touch import Touch

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'touch')
"""The test results directory."""

FNAME = os.path.join(RESULTS, 'empty.txt')
"""The file to create."""

class TestTouch:
    """Touch interface unit tests."""
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
        
    def test_touch(self):
        # Touch the file.
        touch = Touch(fname=FNAME)
        result = touch.run()
        # Verify the result.
        assert_equal(FNAME, result.outputs.fname, "Touch target file name"
            " incorrect: %s" % result.outputs.fname)
        assert_true(os.path.exists(FNAME), "Touch target file does not"
            " exist: %s" % FNAME)
        
        # Retouch the file.
        result = touch.run()
        # Verify the result.
        assert_equal(FNAME, result.outputs.fname, "Touch target file name"
            " incorrect: %s" % result.outputs.fname)
        assert_true(os.path.exists(FNAME), "Touch target file does not"
            " exist: %s" % FNAME)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
