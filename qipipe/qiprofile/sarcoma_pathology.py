"""
This module updates the qiprofile database Subject pathology information
from the pathology Excel workbook file.
"""
import six
import numbers
from qirest_client.model.clinical import (
    SarcomaPathology, FNCLCCGrade, NecrosisPercentValue,
    NecrosisPercentRange, necrosis_percent_as_score
)
from . import parse
from .pathology import (PathologyError, PathologyWorksheet, PathologyUpdate)

COL_ATTRS = {'Tumor Location': 'location'}
"""
The following special column: attribute associations:
* The ``Tumor Location`` column corresponds to the pathology ``location``
  attribute
"""

PARSERS = dict(necrosis_percent=lambda s: _parse_necrosis_percent(s))
"""
The following special parsers:
* The necrosis percent can be an integer or a range, e.g. ``80-90``.
"""


def read(workbook, **condition):
    """
    This is a convenience method that wraps :class:`SarcomaPathologyWorksheet`
    :meth:`qipipe.qiprofile.xls.Worksheet.read`.
    
    :param workbook: the read-only ``openpyxl`` workbook object
    :param condition: the :meth:`qipipe.qiprofile.xls.Worksheet.read`
        filter condition
    :return: the :meth:`qipipe.qiprofile.xls.Worksheet.read` rows
    """
    reader = PathologyWorksheet(workbook, SarcomaPathology, FNCLCCGrade,
                                parsers=dict(PARSERS),
                                column_attributes=COL_ATTRS)
    
    return reader.read(**condition)


def update(subject, rows):
    """
    Updates the given subject data object from the Sarcoma pathology XLS
    rows.
    
    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the input pathology :meth:`read` rows list
    """
    updater = SarcomaPathologyUpdate(subject)
    updater.update(rows)


def _parse_necrosis_percent(value):
    """
    :param value: the input XLS value
    :return: the necrosis percent database object
    :raise qipipe.qiprofile.parse.ParseError: if the value cannot be parsed
    """
    if isinstance(value, numbers.Integral):
        return NecrosisPercentValue(value=int(value))
    elif not value:
        return None
    elif not isinstance(value, six.string_types):
        raise parse.ParseError("The input necrosis percent type is not"
                         " supported: %s (%s)" % (value, value.__class__))
    
    # Parse the string input.
    values = [int(bound) for bound in value.split('-')]
    if len(values) == 1:
        return NecrosisPercentValue(value=values[0])
    else:
        start_val, stop_val = values
        start_bnd = NecrosisPercentRange.LowerBound(value=start_val)
        stop_bnd = NecrosisPercentRange.UpperBound(value=stop_val)
        return NecrosisPercentRange(start=start_bnd, stop=stop_bnd)


class SarcomaPathologyUpdate(PathologyUpdate):
    """The Sarcoma pathology update facade."""
    
    def __init__(self, subject):
        """
        :param subject: the ``Subject`` Mongo Engine database object
            to update
        """
        super(SarcomaPathologyUpdate, self).__init__(
            subject, tumor_type='Sarcoma', pathology_class=SarcomaPathology,
            grade_class=FNCLCCGrade
        )
    
    def pathology_content(self, row):
        """
        Collects the pathology object from the given input row.
        This subclass implementation adds the following items:
        * If there are *necrosis_percent* and *tnm* items, then the TNM
          necrosis_score is inferred from the necrosis percent
        
        :param row: the input row
        :return: the {attribute: value} content dictionary
        """
        # Delegate to PathologyUpdate for the common pathology fields.
        content = super(SarcomaPathologyUpdate, self).pathology_content(row)
        # If there is a necrosis percent, then calculate the necrosis score
        # from the necrosis percent.
        if 'necrosis_percent' in content and 'tnm' in content:
            necrosis_pct = content['necrosis_percent']
            tnm = content['tnm']
            tnm.necrosis_score = necrosis_percent_as_score(necrosis_pct)
        
        return content
