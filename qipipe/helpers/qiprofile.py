"""
This module provides helper methods for updating the qiprofile
REST database.
"""

import qixnat
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.imaging import (
  Session, SessionDetail, Modeling, ModelingProtocol, Scan, ScanProtocol,
  Registration, RegistrationProtocol, LabelMap, Volume)
from qiprofile_rest_client.model.uom import (Measurement, Weight)
from qiprofile_rest_client.model.clinical import (
  Treatment, Drug, Dosage, Biopsy, Surgery, Assessment, GenericEvaluation,
  TNM, BreastPathology, BreastReceptorStatus, HormoneReceptorStatus,
  BreastGeneticExpression, NormalizedAssay, ModifiedBloomRichardsonGrade,
  SarcomaPathology, FNCLCCGrade, NecrosisPercentValue, NecrosisPercentRange)

def sync_session(project, subject, session):
    """
    Updates the qiprofile database from the XNAT database content for the
    given session.
    """
    with qixnat.connect as xnat:
        sbj = xnat.find_session(project, subject, session)
        