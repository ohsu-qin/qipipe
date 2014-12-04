import os
import shutil
from nose.tools import (assert_equal, assert_true)
from qipipe.interfaces.touch import Touch
from ... import ROOT
from ...helpers.logging_helper import logger

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'touch')
"""The test results directory."""

FNAME = 'empty.txt'
"""The name of the file to create."""

PATH = os.path.join(RESULTS, FNAME)
"""The path of the file to create."""


class TestTouch(object):

    """Touch interface unit tests."""

    def setUp(self):
        shutil.rmtree(RESULTS, True)

    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_touch_with_dir(self):
        # Touch the file.
        touch = Touch(in_file=PATH)
        result = touch.run()
        # Verify the result.
        assert_equal(result.outputs.out_file, PATH, "Touch target file name"
                     " incorrect: %s" % result.outputs.out_file)
        assert_true(os.path.exists(PATH), "Touch target file does not"
                    " exist: %s" % PATH)

        # Retouch the file.
        result = touch.run()
        # Verify the result.
        assert_equal(result.outputs.out_file, PATH, "Touch target file name"
                     " incorrect: %s" % result.outputs.out_file)
        assert_true(os.path.exists(PATH), "Touch target file does not"
                    " exist: %s" % PATH)

    def test_touch_without_dir(self):
        os.makedirs(RESULTS)
        prev_wd = os.getcwd()
        os.chdir(RESULTS)
        try:
            # Touch the file.
            touch = Touch(in_file=FNAME)
            result = touch.run()
        finally:
            os.chdir(prev_wd)
        # Verify the result.
        assert_equal(result.outputs.out_file, PATH, "Touch target file name"
                     " incorrect: %s" % result.outputs.out_file)
        assert_true(os.path.exists(PATH), "Touch target file does not"
                    " exist: %s" % PATH)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
