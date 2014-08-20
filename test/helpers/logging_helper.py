"""
This test logging_helper module configures test case logging to print
debug messages to stdout.
"""

from qiutil.logging_helper import (configure, logger)

configure(filename=None, level='DEBUG')
