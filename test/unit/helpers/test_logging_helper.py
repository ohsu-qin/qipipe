from nose.tools import *
import sys, os, shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.helpers import logging_helper
from qipipe.helpers.logging_helper import logger

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURE = os.path.join(ROOT, 'fixtures', 'helpers', 'logging', 'logging.yaml')
"""The test fixture logging configuration file."""

RESULTS = os.path.join(ROOT, 'results', 'helpers', 'logging')
"""The test result parent directory."""

RESULT = os.path.join(RESULTS, 'log', 'qipipe.log')
"""The resulting test log."""

class TestLoggingHelper:
    """The logging unit tests."""
    
    def setUp(self):
        self._logger = logger(__name__)
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
    
    def test_filename(self):
        logging_helper.configure(filename=RESULT)
        self._logger.info("Test log message.")
        assert_true(os.path.exists(RESULT),
            "The log file was not created: %s" % RESULT)
        with open(RESULT) as fs:
            msgs = fs.readlines()
        assert_equal(1, len(msgs), "Extraneous log messages in %s" % RESULT)
    
    def test_level(self):
        logging_helper.configure(filename=RESULT)
        self._logger.info("Test log message.")
        assert_true(os.path.exists(RESULT),
            "The log file was not created: %s" % RESULT)
        with open(RESULT) as fs:
            msgs = fs.readlines()
        assert_equal(1, len(msgs), "Extraneous log messages in %s" % RESULT)
    
    def test_qipipe_logger(self):
        logging_helper.configure(filename=RESULT)
        logger('qipipe').info("Test log message.")
        assert_true(os.path.exists(RESULT),
            "The log file was not created: %s" % RESULT)
        with open(RESULT) as fs:
            msgs = fs.readlines()
        assert_equal(1, len(msgs), "Extraneous log messages in %s" % RESULT)
    
    def test_console_only(self):
        logging_helper.configure(root=dict(handlers=['console']),
                          handlers=dict(console=dict(level='INFO')))
        self._logger.info("Test log message.")
        assert_false(os.path.exists(RESULT),
            "The log file was incorrectly created: %s" % RESULT)

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
