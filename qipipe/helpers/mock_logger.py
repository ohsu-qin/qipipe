import os
from datetime import datetime
import logging

class MockLogger(object):
    """
    This MockLogger prints all log messages to stdout.
    This works around the following Nipype bug:

    * Nipype stomps on any other application logging. The work-around
      is to mock a "logger" that writes to stdout.

    The log message is preceded by the process id.

    Note: Log messages might be interleaved from different nodes
    in a cluster environment.
    """
    def __init__(self, level=None):
        if not level:
            level = 'INFO'
        self.level_s = level
        self.level = getattr(logging, self.level_s)
        self.pid = os.getpid()

    def info(self, message):
        if self.level <= logging.INFO:
            print "%s %s" % (self.prefix(), message)

    def error(self, message):
        if self.level <= logging.ERROR:
            print "%s %s" % (self.prefix(), message)

    def warn(self, message):
        if self.level <= logging.WARN:
            print "%s %s" % (self.prefix(), message)

    def debug(self, message):
        if self.level <= logging.DEBUG:
            print "%s %s" % (self.prefix(), message)

    def prefix(self):
        dt = datetime.now().strftime("%M/%D/%Y %H:%M:%S")
        return "%s (%s) %s" % (dt, self.pid, self.level_s)
