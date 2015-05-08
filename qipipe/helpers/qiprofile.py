"""
This module provides helper methods for updating the qiprofile
REST database.
"""

import re
import csv
from datetime import datetime
import pytz
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

TRAILING_NUM_REGEX = re.compile("\d+$")
"""A regular expression to extract the trailing number from a string."""

VALUE_TYPES = dict(
    treatment=dict(begin_date=datetime)
)


class CSVReader(object):
    """Reads a clinical CSV file filtered on a subject and session."""
    
    def __init__(self, in_file, subject, session):
        """
        :param in_file: the CSV file to read
        :param subject: the XNAT subject name
        :param session: the XNAT session name
        """
        self.file = in_file
        self.subject = subject
        self.session = session

    def next(self):
        """
        :yield a Bunch for the given subject and session
        """
        # The target subject and session numbers.
        tgt_sbj_nbr = TRAILING_NUM_REGEX.match(self.subject)
        tgt_sess_nbr = TRAILING_NUM_REGEX.match(self.session)
        # Read each row.
        with open(in_file, 'rb') as csvfile:
            reader = csv.DictReader(csvfile):
            for row in reader:
                # Match on the subject number.
                row_sbj = row.pop('subject', None) row.pop('patient', None)
                row_sbj_nbr = TRAILING_NUM_REGEX.match(row_sbj)
                if row_sbj_nbr == tgt_sbj_nbr:
                    # If there is a row session, then match on the
                    # session number.
                    row_sess = row.pop('session', None) or row.pop('visit', None)
                    if row_sess:
                        row_sess_nbr = TRAILING_NUM_REGEX.match(row_sess)
                        if row_sess_nbr != tgt_sess_nbr:
                            # No match; skip this row.
                            continue
                    # Make a bunch from the row.
                    bunch = Bunch()
                    bunch.update(row)
                    # Set the subject.
                    bunch.subject = self.subject
                    # If the session occurs in the row, then set the session.
                    if row_sess:
                        bunch.session = self.session
                    
                    # We have a winner.
                    yield bunch


class TreatmentReader(object):
    """Reads the Treatment CSV file."""
    
    def __init__(self):
        

# TODO - make parser for each domain type, e.g.:
#
# Treatment:
# ..., Dosage, ...
# ...,"3mg imatinib 3dx for 30 days starting 04/12/2014, ...", ...
#
# becomes treatment.dosages = [dosage, ...], where:
#
# DRUG_WEIGHT = Weight()
# PERSON_WEIGHT = Weight(scale='k')
# dosage = Dosage(agent=Drug.name('imatinib'),
#                 amount=Measurement(amount=3, unit=DRUG_WEIGHT),
#                 ...)
#

"""
The CSV row attribute value type dictionary. Attributes have the following
default types:
    * An attribute ending in ``date`` has default ``datetime``. 
    * Otherwise, the default type is ``str``. 
Attributes with a default type are not included in this dictionary.
"""

def sync_session(project, subject, session, **opts):
    """
    Updates the qiprofile database from the XNAT database content for the
    given session.
    
    :param project: the XNAT project name
    :param subject: the XNAT subject name
    :param session: the XNAT session name
    :param opts: the following options:
    :keyword pathology: the pathology CSV input file
    :keyword treatment: the treatment CSV input file
    :keyword dosage: the dosage CSV input file
    """
    with qixnat.connect as xnat:
        sbj = xnat.find_session(project, subject, session)


def _load_clinical_files(subject, session, **opts):
    """
    :param subject: the XNAT subject name
    :param session: the XNAT session name
    :param opts: the :meth:`sync_session` options
    :return the {input type, {attribute: value}} dictionary
    """
    # Load each file in the options.
    return {in_type: _load_clinical_file(subject, session, in_type, in_file)
            for in_type, in_file in opts.iteritems()}


def _load_clinical_file(subject, session, in_type, in_file):
    """
    :param subject: the XNAT subject name
    :param session: the XNAT session name
    :param opts: the :meth:`sync_session` options
    """
    # The target subject and session numbers.
    tgt_sbj_nbr = TRAILING_NUM_REGEX.match(subject)
    tgt_sess_nbr = TRAILING_NUM_REGEX.match(session)
    type_dict = VALUE_TYPES.get(in_type, {})

    with open(in_file, 'rb') as csvfile:
        reader = csv.DictReader(csvfile):
        for row in reader:
            row_sbj_nbr = TRAILING_NUM_REGEX.match(row['subject'])
            if row_sbj_nbr == tgt_sbj_nbr:
                return _format_clinical_row(subject, row, type_dict)


def _format_clinical_row(subject, row, type_dict):
    """
    Transforms the CSV subject row values as follows:
    
    * the subject value is the subject parameter
    
    * the numeric values are cast to the appropriate type
    
    :param row: the input CSV row dictionary
    :param type_dict: the {attribute: type} dictionary
    :return: a dictionary with transformed values
    """
    value_dict = dict(subject=subject)
    for attr, value in row_dict.iteritems():
        if attr == 'subject':
            # We already have the correctly formatted subject value.
            continue
        vtype = type_dict.get(attr, None)
        value_dict[attr] = _format_value(attr, value, vtype)
    
    return value_dict


def _format_value(attr, value, vtype=None):
    """
    :param attr: the attribute name
    :param vtype: the value type (or None to use the default)
    :return the value cast to the correct type, if necessary
    """
    if value == None or value == '':
        return None
    elif vtype:
        return vtype(value)
    elif attr.endswith('date'):
        return datetime(value)
    else
        return value
