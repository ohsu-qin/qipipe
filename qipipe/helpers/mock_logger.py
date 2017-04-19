import os
import sys
from datetime import datetime
import logging

class MockLogger(object):
    """
    This MockLogger prints all log messages to stdout.
    This works around the following Nipype bug:

    * Nipype stomps on any other application's logging.
      The work-around is to mock a "logger" that writes
      to stdout.

    The log message is preceded by the date, time and process id.

    Note: Log messages might be interleaved from different nodes
    in a cluster environment. The prefix process id helps
    disambiguate the log context.
    """
    def __init__(self, level=None):
        if not level:
            level = 'INFO'
        self.level_s = level
        self.level = getattr(logging, self.level_s)
        self.pid = os.getpid()
        self.info('Mock logger enabled.')

    def info(self, message):
        if self.level <= logging.INFO:
            self._write(message)

    def error(self, message):
        if self.level <= logging.ERROR:
            self._write(message)

    def warn(self, message):
        if self.level <= logging.WARN:
            self._write(message)

    def debug(self, message):
        if self.level <= logging.DEBUG:
            self._write(message)

    def _write(self, message):
        dt = datetime.now().strftime("%M/%D/%Y %H:%M:%S")
        print "%s (%d) %s %s" % (dt, self.pid, self.level_s, message)
        sys.stdout.flush()
