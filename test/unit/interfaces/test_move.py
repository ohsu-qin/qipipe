import os, shutil
from nose.tools import *
from qipipe.interfaces.move import Move
from test import ROOT

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'move', 'small.txt')
"""The test fixture file."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'move')
"""The test results directory."""

SOURCE = os.path.join(RESULTS, 'source', 'data')
"""The test move source directory."""

TARGET = os.path.join(RESULTS, 'target')
"""The test target area where the work data is moved."""

class TestMove:
    """Move interface unit tests."""
    
    def setUp(self):
        """Sets up the source area."""
        shutil.rmtree(RESULTS, True)
        os.makedirs(SOURCE)
        shutil.copy(FIXTURE, SOURCE)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
        
    def test_move_file(self):
        # The source file.
        src_file = os.path.join(SOURCE, 'small.txt')
        
        # Move the file.
        move = Move(in_file=src_file, dest=TARGET)
        result = move.run()
        
        # Verify the result.
        tgt_file = os.path.join(TARGET, 'small.txt')
        assert_equal(tgt_file, result.outputs.out_file, "Move target file name incorrect: %s" % result.outputs.out_file)
        assert_true(os.path.exists(tgt_file), "Move target file does not exist: %s" % tgt_file)
        assert_false(os.path.exists(src_file), "Move source file still exists: %s" % tgt_file)
    
    def test_move_directory(self):
        # Move the directory.
        move = Move(in_file=SOURCE, dest=TARGET)
        result = move.run()
        
        # Verify the result.
        _, dname = os.path.split(SOURCE)
        tgt_dir = os.path.join(TARGET, dname)
        assert_equal(tgt_dir, result.outputs.out_file, "Move target directory name incorrect: %s" % result.outputs.out_file)
        assert_true(os.path.exists(tgt_dir), "Move target directory does not exist: %s" % tgt_dir)
        assert_false(os.path.exists(SOURCE), "Move source directory still exists: %s" % tgt_dir)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
