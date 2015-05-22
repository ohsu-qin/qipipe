"""
This module provides helper methods for updating the qiprofile
REST database.
"""

from bunch import Bunch
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.uom import (Measurement, Weight)
from qiprofile_rest_client.model.clinical import (Drug, Dosage)
from . import csv
from .helpers import (trailing_number, default_parser)


def filter(filename, subject):
    """
    :param filename: the CSV file location
    :param subject: the XNAT subject name
    :return: the subject demographics rows list
    :raise CSVError: if no CSV row matched the subject
    """
    with csv.read(filename, parser=_dosage_parser) as reader:
        return list(reader.filter(subject))


def prepare(project, collection, row):
    """
    :param project: The XNAT project name
    :param collection: the image collection name
    :param row: the filtered CSV input {attribute: value} dictionary rows
    :return: the unsaved ``Dosage`` database objects list
    """
    # Start with the project and collection.
    db_dict = dict(project=project, collection=collection)
    # Add the CSV fields.
    db_dict.update(row)
    # Make the subject database object.
    subject = Subject(**db_dict)
    # Save the subject.
    subject.save()
    
    # Return the subject database object.
    return subject


def _dosage_parser(attribute):
    # The agent is lower case.
    dosage_parsers = dict(agent=lambda s: s.lower())
    
    return dosage_parsers.get(attribute, default_parser(attribute))
