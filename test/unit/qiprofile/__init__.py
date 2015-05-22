"""
The qiprofile update tests.

:Note: most of the test cases require that the MongoDB and qiprofile REST
    servers are running on localhost.
"""

import os
from bunch import Bunch
from ... import (ROOT, PROJECT)

PROJECT = 'QIN_Test'
"""The test project."""

BREAST_SUBJECT = 'Breast001'
"""The Breast test subject."""

SARCOMA_SUBJECT = 'Sarcoma001'
"""The Sarcoma test subject."""

SESSION = 'Session01'
"""The test session."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'qiprofile')

BREAST_FIXTURES = Bunch(
    demographics=os.path.join(FIXTURES, 'breast', 'demographics.csv'),
    dosage=os.path.join(FIXTURES, 'breast', 'dosage.csv'),
    pathology=os.path.join(FIXTURES, 'breast', 'pathology.csv'),
    treatment=os.path.join(FIXTURES, 'breast', 'treatment.csv'),
    visit=os.path.join(FIXTURES, 'breast' , 'visit.csv')
)

SARCOMA_FIXTURES = Bunch(
    demographics=os.path.join(FIXTURES, 'sarcoma', 'demographics.csv'),
    dosage=os.path.join(FIXTURES, 'sarcoma','dosage.csv'),
    pathology=os.path.join(FIXTURES, 'sarcoma', 'pathology.csv'),
    treatment=os.path.join(FIXTURES, 'sarcoma', 'treatment.csv'),
    visit=os.path.join(FIXTURES, 'sarcoma', 'visit.csv')
)

