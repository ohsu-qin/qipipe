# Absolute import (the default in a future Python release) resolves
# the logging import as the Python standard logging module rather
# than this module of the same name.
from __future__ import absolute_import
import os
import sys
from datetime import datetime
import logging
import qiutil

NIPYPE_LOG_DIR_ENV_VAR = 'NIPYPE_LOG_DIR'
"""The environment variable used by Nipype to set the log directory."""


def configure(**opts):
    """
    Configures the logger as follows:

    - If there is a *log* option,
      then the logger is a conventional ``qiutil.logging`` logger
      which writes to the given log file.

    - Otherwise, the logger delegates to a mock logger that
      writes to stdout.

    :Note: In a cluster environment, Nipype kills the dispatched job
    log config. Logging falls back to the default. For this reason,
    the default mock logger level is ``DEBUG`` rather than ``INFO``.
    The dispatched node's log is the stdout captured in the file
    *work*\ ``/batch/``\ *node_name*\ ``.o``\ *node_id*, where
    *work* the execution work directory.

    :param opts: the ``qiutil.command.configure_log`` options
    :return: the logger factory
    """
    # The log file option.
    log_file_opt = opts.get('log')
    # Set the Nipype log directory environment variable before importing
    # any nipype module. The code below works around the following Nipype
    # bug:
    # * Nipype requires a log directory. If the Nipype log directory is
    #   set to /dev/null, then Nipype raises an error. The work-around
    #   is to set the NIPYPE_LOG_DIR environment variable to a new temp
    #   directory.
    log_dir = None
    if log_file_opt:
        # Configure the qiutil logger for the qi* modules.
        qiutil.command.configure_log('qipipe', 'qixnat', 'qidicom',
                                     'qiutil', **opts)
        log_file = os.path.abspath(log_file_opt)
        if log_file == '/dev/null':
            # Work around the Nipype bug described above.
            log_dir = tempfile.mkdtemp(prefix='qipipe_')
        else:
            log_dir = os.path.dirname(log_file)
        # Make the log file parent directory, if necessary.
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        factory = qiutil.logging.logger
    else:
        # Print log messages to stdout to work around the Nipype
        # bug described in the logger method apidoc.
        mock_log_opts = {}
        level = opts.get('log_level')
        if level:
            mock_log_opts['level'] = level
        factory = MockLoggerFactory(**mock_log_opts).logger

    # Nipype always needs a log directory to work around the
    # following Nipype bug:
    # * If the Nipype log directory is not set, then Nipype still
    #   logs to the default log file ./log/pypeline.log, but also
    #   logs to stdout, which stomps on the qipipe logging.
    if not log_dir:
        log_dir = '/'.join([os.getcwd(), 'log'])
    # Set the Nipype log directory environment variable.
    os.environ[NIPYPE_LOG_DIR_ENV_VAR] = log_dir

    # Set the global logger factory.
    logger._factory = factory
    # Print a log message.
    log_dest = log_file_opt if log_file_opt else 'stdout'
    log_level = opts.get('log_level', 'DEBUG')
    factory(__name__).info("Logging qipipe to %s with level %s." %
                           (log_dest, log_level))
    factory(__name__).info("Logging nipype to the %s directory." %
                           log_dir)

    return factory

def logger(name):
    """
    This method overrides ``qiutil.logging.logger`` to work
    around the following Nipype bug:

    * Nipype stomps on any other application's logging.
      The work-around is to mock a "logger" that writes
      to stdout.

    :param name: the caller's context ``__name__``
    :return: the logger facade
    """
    # Make a default logger factory on demand.
    if not hasattr(logger, '_factory'):
        logger._factory = configure()

    return logger._factory(name)


class MockLoggerFactory(object):
    def __init__(self, **opts):
        self.writer = MockLogWriter(**opts)

    def logger(self, name):
        return MockLogger(self.writer, name)


class MockLogger(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name

    @property
    def level(self):
        return self.writer.level

    def info(self, message):
        self.writer.info(self.name, message)

    def error(self, message):
        self.writer.error(self.name, message)

    def warn(self, message):
        self.writer.warn(self.name, message)

    def debug(self, message):
        self.writer.debug(self.name, message)


class MockLogWriter(object):
    def __init__(self, level=None):
        if not level:
            level = 'DEBUG'
        self.level = getattr(logging, level)

    def info(self, name, message):
        if self.level <= logging.INFO:
            self._write(name, 'INFO', message)

    def debug(self, name, message):
        if self.level <= logging.DEBUG:
            self._write(name, 'DEBUG', message)

    def warn(self, name, message):
        if self.level <= logging.WARN:
            self._write(name, 'WARN', message)

    def error(self, name, message):
        if self.level <= logging.ERROR:
            self._write(name, 'ERROR', message)

    def _write(self, name, level, message):
        dt = datetime.now().strftime("%M/%D/%Y %H:%M:%S")
        print "%s %s %s %s" % (dt, name, level, message)
        sys.stdout.flush()
