"""
This module updates the qiprofile database Subject pathology information
from the pathology Excel workbook file.
"""
from qirest_client.model.clinical import (
    Surgery, BreastSurgery, BreastPathology, ModifiedBloomRichardsonGrade,
    ResidualCancerBurden, HormoneReceptorStatus, BreastGeneticExpression
)
from . import parse
from .pathology import (PathologyError, PathologyWorksheet, PathologyUpdate)

HORMONES = ['estrogen', 'progesterone']
"""The receptor status hormones."""



def read(workbook, **condition):
    """
    This is a convenience method that wraps :class:`BreastPathologyWorksheet`
    :meth:`qipipe.qiprofile.xls.Worksheet.read`.
    
    :param workbook: the read-only ``openpyxl`` workbook object
    :param condition: the :meth:`qipipe.qiprofile.xls.Worksheet.read`
        filter condition
    :return: the :meth:`qipipe.qiprofile.xls.Worksheet.read` rows
    """
    reader = PathologyWorksheet(workbook, BreastSurgery, BreastPathology,
                                ModifiedBloomRichardsonGrade,
                                ResidualCancerBurden,
                                BreastGeneticExpression,
                                parsers=_receptor_parsers())
    
    return reader.read(**condition)


def update(subject, rows):
    """
    Updates the given subject data object from the Breast pathology XLS
    rows.
    
    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the input pathology :meth:`read` rows list
    """
    updater = BreastPathologyUpdate(subject)
    updater.update(rows)


class BreastPathologyUpdate(PathologyUpdate):
    """The Breast pathology update facade."""
    
    def __init__(self, subject):
        """
        :param subject: the ``Subject`` Mongo Engine database object
            to update
        """
        super(BreastPathologyUpdate, self).__init__(
            subject, tumor_type='Breast', pathology_class=BreastPathology,
            grade_class=ModifiedBloomRichardsonGrade
        )
    
    def encounter_type(self, row):
        """
        Overrides :meth:`qipipe.qiprofile.Pathology.encounter_type` to
        specialize the *intervention_type* to ``BreastSurgery``.
        
        :param row: the input row
        :return: the REST data model Encounter subclass
        """
        base_type = super(BreastPathologyUpdate, self).encounter_type(row)
        
        return BreastSurgery if base_type == Surgery else base_type
    
    def pathology_content(self, row):
        """
        Collects the pathology object from the given input row.
        This subclass implementation adds the non-empty embedded
        fields specific to this tumor type.
        
        :param row: the input row
        :return: the {attribute: value} content dictionary
        """
        # Delegate to PathologyUpdate for the common pathology fields.
        content = super(BreastPathologyUpdate, self).pathology_content(row)
        # The receptor status objects.
        rcptrs = _collect_hormone_receptors(row)
        if rcptrs:
            content['hormone_receptors'] = rcptrs
        # The RCB {attribute: value} content.
        rcb_content = _rcb_content(row)
        if rcb_content:
            rcb = ResidualCancerBurden(**rcb_content)
            content['rcb'] = rcb
        # The genetic expression {attribute: value} content.
        ge_content = _gene_expression_content(row)
        if ge_content:
            ge = BreastGeneticExpression(**ge_content)
            content['genetic_expression'] = ge
        
        return content


def _collect_hormone_receptors(row):
    """
    :param row: the input row
    :return: the non-empty receptor statuses
    """
    rcptrs = []
    for hormone in HORMONES:
        content = _hormone_receptor_content(hormone, row)
        if content:
            rcptr_status = HormoneReceptorStatus(**content)
            rcptr_status.append(rcptr)
    
    return rcptrs


def _hormone_receptor_content(hormone, row):
    """
    :param hormone: the target hormone
    :param row: the input row
    :return: the {attribute: value} dictionary for non-None row values
    """
    # The result {attribute: value} dictionary.
    content = {}
    # The fields to collect from the row, excluding hormone which
    # we already have.
    fields = (field for field in HormoneReceptorStatus._fields
              if field in row)
    # Collect the values.
    for field in fields:
        row_attr = '_'.join([hormone, field])
        val = row[row_attr]
        if val != None:
            content[field] = val
    
    return content


def _rcb_content(row):
    """
    :param row: the input row
    :return: the {attribute: value} dictionary for non-None row values
    """
    return {attr: row[attr] for attr in ResidualCancerBurden._fields
            if attr in row and row[attr] != None}


def _gene_expression_content(row):
    """
    :param row: the input row
    :return: the {attribute: value} dictionary for non-None row values
    """
    return {attr: row[attr] for attr in BreastGeneticExpression._fields
            if attr in row and row[attr] != None}


def _receptor_parsers():
    """
    The hormone receptor input fields are prefixed by the hormone name,
    e.g. ``estrogen_positive`` is the ``HormoneReceptorStatus.positive``
    field for the ``HormoneReceptorStatus`` with name ``estrogen``.
    The parsers for the hormone receptor input fields are thus given by
    the respective default ``HormoneReceptorStatus`` parsers.
    
    :return: the parser {attribute: function} dictionary
    """
    parsers = {}
    def_receptor_parsers = parse.default_parsers(HormoneReceptorStatus)
    for hormone in HORMONES:
        for field, parser in def_receptor_parsers.iteritems():
            attr = '_'.join([hormone, field])
            parsers[attr] = parser
    
    return parsers
