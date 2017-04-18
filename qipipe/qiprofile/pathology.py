"""
This module updates the qiprofile database Subject pathology information
from the pathology Excel workbook file.
"""
from bunch import bunchify
from qirest_client.model.common import (Encounter, TumorExtent)
from qirest_client.model.clinical import (
    Biopsy, Surgery, PathologyReport, TNM, TumorLocation
)
from . import parse
from .xls import Worksheet

ENCOUNTER_TYPES = {klass.__name__: klass for klass in (Biopsy, Surgery)}
"""The encounter {name: class} dictionary."""

COL_ATTRS = {
    'Patient Weight (kg)': 'weight',
    'Tumor Size Score': 'size',
    'Tumor Length (mm)': 'length',
    'Tumor Width (mm)': 'width',
    'Tumor Depth (mm)': 'depth'
}
"""
The following non-standard column-attribute associations:

* ``Patient Weight (kg)``: *Encounter.weight* attribute
* ``Tumor Size Score``: *TNM.size* attribute
* ``Tumor Length (mm)``: *TumorExtent.length* attribute
* ``Tumor Width (mm)``: *TumorExtent.width* attribute
* ``Tumor Depth (mm)``: *TumorExtent.depth* attribute
"""

PARSERS = dict(
    subject_number=int,
    lesion_number=int,
    body_part=lambda value: value.lower().capitalize(),
    # Wrap the functions below with a lambda as a convenience to allow
    # a forward reference to the parse functions defined later.
    intervention_type=lambda value: _parse_intervention_type(value),
    size=lambda value: _parse_tumor_size(value)
)
"""
The following parser associations:

* *subject_number* is an int
* *intervention_type* converts the string to an Encounter subclass
* *body_part* is capitalized
* *size* is a :class:`qirest_client.clinical.TNM.Size` object
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
        :param classes: the subclass-specific REST data model subclasses
        :param opts: the following keyword arguments:
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
            workbook, SHEET, Encounter, TumorLocation, TumorExtent, TNM,
            *classes, parsers=parsers, column_attributes=col_attrs
        )


class PathologyUpdate(object):
    """The pathology update abstract class."""
    
    def __init__(self, subject, tumor_type, grade_class, pathology_class):
        """
        :param subject: the ``Subject`` Mongo Engine database object
            to update
        :param tumor_type: the subclass tumor type
        :option pathology_class: the REST data model TumorPathology
            subclass
        :option grade_class: the REST data model Grade subclass
        """
        self._subject = subject
        self._tumor_type = tumor_type
        data_model_dict = dict(grade=grade_class, pathology=pathology_class)
        self._data_models = bunchify(data_model_dict)
    
    def update(self, rows):
        """
        Updates the subject data object from the given pathology XLS
        rows.
        
        :param rows: the input pathology :meth:`read` rows list
        """
        # Update the encounter rows.
        for enc_rows in self._encounter_rows(rows):
            self._update_encounter(enc_rows)
    
    def _encounter_rows(self, rows):
        """
        Partition the rows by encounter.
        
        :param rows: the input rows
        :yield: each encounter's lesion rows
        """
        enc_rows = []
        for row in rows:
            if row.lesion_number == 1 and enc_rows:
                yield enc_rows
                enc_rows = []
            enc_rows.append(row)
        if enc_rows:
            yield enc_rows
    
    def _update_encounter(self, rows):
        """
        :param rows: the input pathology :meth:`read` rows for the
            encounter
        """
        # The lesion 1 row has the encounter information.
        master_row = rows[0]
        # There must be an intervention type.
        if not master_row.intervention_type:
            raise PathologyError("The pathology input row is missing the"
                                 "intervention type")
        if not master_row.date:
            raise PathologyError("The pathology input row is missing the"
                                 "date")
        # The encounter object for the input encounter type.
        enc_type = self.encounter_type(master_row)
        enc = self._encounter_for(enc_type, master_row.date)
        # Update the encounter.
        self.update_encounter(enc, rows)
    
    def update_encounter(self, encounter, rows):
        """
        Update the encounter object from the given input row.
        This base implementation sets the encounter attribute values
        from the matching input row attribute value and calls
        :meth:`update_pathology` to update the pathology.
        Other updates are a subclass responsibility.
        
        :param encounter: the encounter object
        :param rows: the input pathology :meth:`read` rows for the
            encounter
        """
        # The lesion 1 row has the encounter information.
        master_row = rows[0]
        for attr in encounter.__class__._fields:
            if attr in master_row:
                setattr(encounter, attr, master_row[attr])
        # Delegate to the possibly overridden pathology_content
        # method to collect the pathology content.
        path_content = (self.pathology_content(row) for row in rows)
        tumors = [self._data_models.pathology(**content)
                  for content in path_content if path_content]
        if tumors:
            encounter.pathology = PathologyReport(tumors=tumors)
    
    def pathology_content(self, row):
        """
        Collects the TumorPathology content from the given input row.
        This base implementation collects the pathology attribute
        values from the matching input row attribute value. Other
        updates are a subclass responsibility.
        
        :param row: the input row
        :return: the {attribute: value} content dictionary
        """
        # The TumorPathology subclass.
        path_cls = self._data_models.pathology
        # The non-embedded content.
        path_content = {attr: row[attr] for attr in path_cls._fields
                        if attr in row and row[attr] != None}
        # The TNM non-embedded content.
        tnm_content = {attr: row[attr] for attr in TNM._fields
                       if attr in row and row[attr] != None}
        # THe TNM grade.
        grade_cls = self._data_models.grade
        grade_content = {attr: row[attr] for attr in grade_cls._fields
                         if attr in row and row[attr] != None}
        if grade_content:
            tnm_content['grade'] = grade_cls(**grade_content)
        # The TNM.
        if tnm_content:
            path_content['tnm'] = TNM(tumor_type=self._tumor_type,
                                      **tnm_content)
        # The tumor location.
        location_content = {attr: row[attr] for attr in TumorLocation._fields
                            if row.get(attr) != None}
        if location_content:
            path_content['location'] = TumorLocation(**location_content)
        # The tumor extent. The row.get condition ignores both zero
        # and None values.
        extent_content = {attr: row[attr] for attr in TumorExtent._fields
                          if row.get(attr)}
        if extent_content:
            path_content['extent'] = TumorExtent(**extent_content)
        
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
