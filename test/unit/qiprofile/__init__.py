"""
The qiprofile update tests.

:Note: most of the test cases require that the MongoDB and qiprofile REST
    servers are running on localhost.
"""

import os
from ... import ROOT

DATABASE = 'qipipe_test'
"""The test REST database name."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'qiprofile')
"""The parent test fixture directory."""

BREAST_FIXTURE = os.path.join(FIXTURES, 'breast', 'clinical.xlsx')
"""The Breast clinical input test fixture."""

SARCOMA_FIXTURE = os.path.join(FIXTURES, 'sarcoma', 'clinical.xlsx')
"""The Sarcoma clinical input test fixture."""
