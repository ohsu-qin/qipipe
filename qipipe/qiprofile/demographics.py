"""
This module provides helper methods for updating the qiprofile
REST database.
"""

import re
from bunch import Bunch
from mongoengine import connect
from qiprofile_rest_client.model.subject import Subject
from . import csv
from .helpers import (trailing_number, default_parser)


class DemographicsError(Exception):
    pass

def read(filename, subject):
    """
    :param filename: the CSV file location
    :param subject: the XNAT subject name
    :return: the subject demographics row
    :rtype: Bunch
    :raise DemographicsError: if more than one CSV row matches
        the subject
    """
    with csv.read(filename, parser=_demographics_parser) as reader:
        rows = list(reader.filter(subject))
    if len(rows) > 1:
        sbj_nbr = trailing_number(self.subject)
        raise DemographicsError("Subject number %d has more than one row in"
                                " the CSV file %s" % (sbj_nbr, filename))

    return rows[0] if rows else None


def _demographics_parser(attribute):
    demog_parsers = dict(races=_parse_races)
    
    return demog_parsers.get(attribute, default_parser(attribute))


def _parse_races(value):
    """
    Example:
    >> _parse_races('White, Asian')
    ['White', 'Asian']
    
    :param value: the CSV input races value
    :return: the list of races
    """
    return [w.strip() for w in value.split(',\w*')]
