"""
The qiprofile update tests.

:Note: most of the test cases require that the MongoDB and qiprofile REST
    servers are running on localhost.
"""

import os
from ... import (ROOT, PROJECT)

PROJECT = 'QIN_Test'
"""The test project."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'qiprofile')

BREAST_FIXTURE = os.path.join(FIXTURES, 'breast', 'clinical.xlsx')

SARCOMA_FIXTURE = os.path.join(FIXTURES, 'sarcoma', 'clinical.xlsx')
