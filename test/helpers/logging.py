"""
This test logging module configures test case logging to print
debug messages to stdout.
"""
# Absolute import (the default in a future Python release) resolves
# the logging import as the Python standard logging module rather
# than this module of the same name.
from __future__ import absolute_import
import logging
from qiutil.logging import (configure, logger)

configure('test', 'qipipe', 'qixnat', 'qidicom', 'qiutil', level='DEBUG')

# Suppress warnings, esp. the dependent libraries comparison to None.
logging.captureWarnings(True)
