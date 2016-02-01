"""
This module updates the qiprofile database clinical information
from the clinical Excel workbook file.
"""
from . import (xls, demographics, breast_pathology, sarcoma_pathology,
               chemotherapy, radiotherapy)


class ClinicalError(Exception):
    pass


def update(subject, filename):
    """
    Updates the subject clinical database content from the given workbook
    file.
    
    :param subject: the target qiprofile Subject to update
    :param filename: the input file location
    """
    wb = xls.load_workbook(filename)
    pathology_module = _pathology_module(subject.collection)
    for module in [demographics, pathology_module, chemotherapy, radiotherapy]:
        rows = module.read(wb, subject_number=subject.number)
        module.update(subject, rows)
    subject.save()


def _pathology_module(collection):
    """
    :param collection: the subject collection name
    :return: the pathology module
    :raise ClinicalError: if the collection does not have a pathology module
    """
    if collection.lower() == 'breast':
        return breast_pathology
    elif collection.lower() == 'sarcoma':
        return sarcoma_pathology
    else:
        raise ClinicalError("Pathology collection not supported: %s" %
                             collection)
