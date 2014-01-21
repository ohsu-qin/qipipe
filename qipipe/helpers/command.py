"""Command helper functions."""

import os
import logging

NIPYPE_LOG_DIR_ENV_VAR = 'NIPYPE_LOG_DIR'
"""The Nipype log directory environment variable."""


def add_standard_options(parser):
    # The log options
    add_log_options(parser)
    
    # The XNAT configuration option.
    parser.add_argument('-c', '--config', help='the XNAT configuration file',
                        metavar='FILE')


def add_log_options(parser):
    """
    Adds the standard --log, --quiet, --verbose and --debug options to the
    given command line arugment parser.
    """
    parser.add_argument('-l', '--log', help='the log file', metavar='FILE')
    verbosity_grp = parser.add_mutually_exclusive_group()
    verbosity_grp.add_argument(
        '-q', '--quiet', help="only log error messages", dest='log_level',
        action='store_const', const=logging.ERROR)
    verbosity_grp.add_argument(
        '-d', '--debug', help='log debug messages', dest='log_level',
        action='store_const', const=logging.DEBUG)


def configure_log(opts):
    """
    Configures the logger. This method should be called before importing
    any qipipe or nipype module.

    :param opts: the following keyword options:
    :keyword log: the log file
    :keyword log_level: the log level
    """
    log_cfg = {}
    if 'log' in opts:
        log_file = os.path.abspath(opts.pop('log'))
        log_cfg['filename'] = log_file
        # Set the Nipype log directory environment variable
        # before importing qipipe. See the comments below.
        log_dir = os.path.dirname(log_file)
        if log_dir:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            os.environ[NIPYPE_LOG_DIR_ENV_VAR] = log_dir
    if 'log_level' in opts:
        log_cfg['level'] = opts.pop('log_level')
    # The Nipype log directory environment variable must be
    # set before importing a nipype module which uses the
    # nipype logger. qipipe imports the Nipype pipeline module.
    # Therefore, import qipipe.helpers here after setting the
    # log directory environment variable above.
    from qipipe.helpers import logging_helper
    # Make the qipipe logger.
    logging_helper.configure(**log_cfg)
