import os

ROOT = os.path.normpath(os.path.dirname(__file__))
"""The test parent directory."""

PROJECT = 'QIN_Test'
"""
The test XNAT project name.

:Note: this test project must be created in XNAT prior to running the
    tests.
"""

CONF_DIR = os.path.join(ROOT, 'conf')
"""The test workflow configurations location."""
