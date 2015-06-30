import os
import shutil
from nose.tools import (assert_equal, assert_true)
from qipipe.interfaces.copy import Copy
from ... import ROOT
from ...helpers.logging import logger

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'copy')
"""The test fixture file."""

SOURCE = os.path.join(FIXTURE, 'small.txt')
"""The test fixture file."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'copy')
"""The test results directory."""

TARGET = os.path.join(RESULTS, 'target')
"""The test target area where the work data is copied."""



class TestCopy(object):
    """Copy interface unit tests."""

    def setUp(self):
        shutil.rmtree(RESULTS, True)

    def tearDown(self):
        pass #shutil.rmtree(RESULTS, True)

    def test_copy_file(self):
        # Copy the file.
        copy = Copy(in_file=SOURCE, dest=TARGET)
        result = copy.run()
    
        # Verify the result.
        tgt_file = os.path.join(TARGET, 'small.txt')
        assert_equal(result.outputs.out_file, tgt_file,
                     "Copy target file name incorrect: %s" %
                     result.outputs.out_file)
        assert_true(os.path.exists(tgt_file),
                    "Copy target file does not exist: %s" % tgt_file)

    def test_copy_file_with_output_filename(self):
        # Copy the file.
        copy = Copy(in_file=SOURCE, dest=TARGET, out_fname='target.txt')
        result = copy.run()
    
        # Verify the result.
        tgt_file = os.path.join(TARGET, 'target.txt')
        assert_equal(result.outputs.out_file, tgt_file,
                     "Copy target file name incorrect: %s" %
                     result.outputs.out_file)
        assert_true(os.path.exists(tgt_file),
                    "Copy target file does not exist: %s" % tgt_file)

    def test_copy_directory(self):
        # Copy the directory.
        copy = Copy(in_file=FIXTURE, dest=TARGET)
        result = copy.run()

        # Verify the result.
        _, dname = os.path.split(FIXTURE)
        tgt_dir = os.path.join(TARGET, dname)
        assert_equal(result.outputs.out_file, tgt_dir,
                     "Copy target directory name incorrect: %s" %
                     result.outputs.out_file)
        tgt_file = os.path.join(tgt_dir, 'small.txt')
        assert_true(os.path.exists(tgt_file),
                    "Copy target directory content is missing: %s" % tgt_file)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
