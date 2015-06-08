# import re
# import xls
# from datetime import datetime
# from bunch import Bunch
# from qiprofile_rest_client.model.subject import Subject
# from qiprofile_rest_client.model.imaging import (
#   Session, SessionDetail, Modeling, ModelingProtocol, Scan, ScanProtocol,
#   Registration, RegistrationProtocol, LabelMap, Volume)
# from qiprofile_rest_client.model.uom import (Measurement, Weight)
# from qiprofile_rest_client.model.clinical import (
#   Treatment, Drug, Dosage, Biopsy, Surgery, Assessment, GenericEvaluation,
#   TNM, BreastPathology, BreastReceptorStatus, HormoneReceptorStatus,
#   BreastGeneticExpression, NormalizedAssay, ModifiedBloomRichardsonGrade,
#   SarcomaPathology, FNCLCCGrade, NecrosisPercentValue, NecrosisPercentRange)

from . import (demographics, dosage)


def sync_session(project, collection, subject, session, **opts):
    """
    Updates the qiprofile database from the XNAT database content for
    the given session.

    :param project: the XNAT project name
    :param collection: the image collection name
    :param subject: the XNAT subject name
    :param session: the XNAT session name, without subject prefix
    :param opts: the following options:
    :keyword demographics: the demographics XLS input file
    :keyword pathology: the pathology XLS input file
    :keyword treatment: the treatment XLS input file
    :keyword dosage: the dosage XLS input file
    :keyword visit: the visit XLS input file
    """
    # Get or create the database subject.
    subject = Subject.objects.get_or_create(project=project,
                                            collection=collection,
                                            number=subject)
    # Set the subject demographics, if it is available.
    demog_file = opts.get('demographics')
    if demog_file:
        demog_row = demographics.filter(demog_file, subject)
        if demog_row:
            for attr, val in demog_row:
                setattr(subject, attr, val)

    # Add the subject encounters, if available.
    enc_file = opts.get('encounters')
    if enc_file:
        subject.encounters
        for row in encounters.read(enc_file, subject):
            

