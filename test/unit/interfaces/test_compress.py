import os, sys, shutil
from nose.tools import *
from qipipe.interfaces.compress import Compress

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'interfaces', 'compress', 'small.txt')
"""The test fixture file."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'compress')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestCompress:
    """Compress interface unit tests."""
    
    def test_compress(self):
        shutil.rmtree(RESULTS, True)
        compress = Compress(in_file=FIXTURE, dest=RESULTS)
        target = os.path.join(RESULTS, 'small.txt.gz')
        result = compress.run()
        assert_equal(target, result.outputs.out_file, "Compress output file"
            " name incorrect: %s" % result.outputs.out_file)
        assert_true(os.path.exists(target))
        shutil.rmtree(RESULTS, True)


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
