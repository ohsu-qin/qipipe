"""
This module updates the qiprofile database Subject pathology information
from the pathology Excel workbook file.
"""
from qiprofile_rest_client.model.clinical import (
    Encounter, Biopsy, Surgery, Pathology, TNM
)
from . import parse
from .xls import Worksheet

ENCOUNTER_TYPES = {klass.__name__: klass for klass in (Biopsy, Surgery)}
"""The encounter {name: class} dictionary."""

COL_ATTRS = {'Patient Weight': 'weight', 'Tumor Size': 'size'}
"""
The following non-standard column-attribute associations:
* The ``Patient Weight`` column is the ``Encounter.weight`` attribute
* The ``Tumor Size`` column is the ``TNM.size`` attribute
"""

PARSERS = dict(
    subject_number=int,
    # Wrap the functions below with a lambda as a convenience to allow
    # a forward reference to the parse functions defined below.
    intervention_type=lambda v: _parse_intervention_type(v),
    size=lambda v: _parse_tumor_size(v)
)
"""
The following parser associations:
* subject_number is an int
* intervention_type converts the string to an Encounter subclass
* size is a :class:`qiprofile_rest_client.clinical.TNM.Size` object
"""

SHEET = 'Pathology'
"""The worksheet name."""


class PathologyError(Exception):
    pass


def _parse_intervention_type(value):
    """
    :param value: the input string
    :return: the encounter class
    """
    value = value.capitalize()
    klass = ENCOUNTER_TYPES.get(value, None)
    if not klass:
        raise PathologyError("The pathology row intervention type is not"
                             " recognized: %s" % value)
    
    return klass


def _parse_tumor_size(value):
    return TNM.Size.parse(str(value))


class PathologyWorksheet(Worksheet):
    """The Pathology worksheet facade."""
    
    def __init__(self, workbook, *classes, **opts):
        """
        :param workbook: the :class:`qipipe.qiprofile.xls.Workbook` object
        :param classes: the subclass-specific classes, including the
            Pathology subclass and the Grade subclass
        :param opts: the following options:
        :option parsers: the non-standard parsers {attribute: function}
            dictionary
        :option column_attributes: the non-standard {column name: attribute}
            dictionary
        """
        # The special parsers.
        parsers = PARSERS.copy()
        # Add the subclass special parsers.
        parsers_opt = opts.get('parsers', None)
        if parsers_opt:
            parsers.update(parsers_opt)
        # The special column-attribute associations.
        col_attrs = COL_ATTRS.copy()
        # Add the subclass special associations.
        col_attrs_opt = opts.get('column_attributes', None)
        if col_attrs_opt:
            col_attrs.update(col_attrs_opt)
        # Initialize the worksheet.
        super(PathologyWorksheet, self).__init__(
            workbook, SHEET, Encounter, TNM, *classes, parsers=parsers,
            column_attributes=col_attrs
        )


class PathologyUpdate(object):
    """The pathology update abstract class."""
    
    def __init__(self, subject, tumor_type, pathology_class, grade_class):
        """
        :param subject: the ``Subject`` Mongo Engine database object
            to update
        :param tumor_type: the subclass tumor type
        :param pathology_class: the Pathology subclass 
        :param grade_class: the Grade subclass 
        """
        self._subject = subject
        self._tumor_type = tumor_type
        self._pathology_class = pathology_class
        self._grade_class = grade_class

    def update(self, rows):
        """
        Updates the subject data object from the given pathology XLS rows.

        :param rows: the input pathology :meth:`read` rows list 
        """
        prev_row = None
        for row in rows:
            self._update(row, prev_row)
            prev_row = row

    def _update(self, row, previous_row):
        """
        :param row: the input pathology :meth:`read` row
        :param previous_row: the preceding 
        """
        # There must be an intervention type.
        if not row.intervention_type:
            raise PathologyError("The pathology input row is missing the"
                                 "intervention type")
        if not row.date:
            raise PathologyError("The pathology input row is missing the"
                                 "date")
        # The encounter object for the input encounter type.
        enc_type = self.encounter_type(row)
        enc = self._encounter_for(enc_type, row.date)
        # Update the encounter.
        self.update_encounter(enc, row)

    def update_encounter(self, encounter, row):
        """
        Update the encounter object from the given input row.
        This base implementation sets the encounter attribute values
        from the matching input row attribute value and calls
        :meth:`update_pathology` to update the pathology.
        Other updates are a subclass responsibility.
        
        :param encounter: the encounter object
        :param row: the input row
        """
        for attr in encounter.__class__._fields:
            if attr in row:
                setattr(encounter, attr, row[attr])
        # Delegate to the possibly overridden update_pathology
        # method to collect the pathology content.
        path_content = self.pathology_content(row)
        if path_content:
            encounter.pathology = self._pathology_class(**path_content)
        else:
            encounter.pathology = None

    def pathology_content(self, row):
        """
        Collects the pathology object from the given input row.
        This base implementation collects the pathology attribute values
        from the matching input row attribute value. Other updates
        are a subclass responsibility.
        
        :param row: the input row
        :return: the {attribute: value} content dictionary
        """
        grade_content = {attr: row[attr] for attr in self._grade_class._fields
                         if attr in row and row[attr] != None}
        tnm_content = {attr: row[attr] for attr in TNM._fields
                       if attr in row and row[attr] != None}
        if grade_content:
            tnm_content['grade'] = self._grade_class(**grade_content)
        path_content = {attr: row[attr] for attr in self._pathology_class._fields
                        if attr in row and row[attr] != None}
        if tnm_content:
            path_content['tnm'] = TNM(tumor_type=self._tumor_type, **tnm_content)
        
        return path_content

    def _encounter_for(self, klass, date):
        """
        :param klass: the encounter class
        :param date: the encounter date
        :return: the existing or new encounter object
        """
        # The encounter list
        encs = self._subject.encounters
        # Find the matching encounter, if it exists.
        enc_iter = (enc for enc in encs
                    if isinstance(enc, klass) and enc.date == date)
        target = next(enc_iter, None)
        if not target:
            # Make the new encounter.
            target = klass(date=date)
            # Add the new encounter to the subject encounters list.
            self._subject.add_encounter(target)

        # Return the target encounter.
        return target

    def encounter_type(self, row):
        """
        Infers the encounter type from the given row. This base
        implementation returns the parsed row *intervention_type*
        value.

        :param row: the input row
        :return: the REST data model Encounter subclass
        """
        return row.intervention_type
