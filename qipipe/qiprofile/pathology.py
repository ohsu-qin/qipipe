"""
This module updates the qiprofile database Subject pathology information
from the pathology Excel workbook file.
"""

from functools import partial
import six
from qiprofile_rest_client.model.clinical import (
    Biopsy, TNM, BreastPathology, ModifiedBloomRichardsonGrade,
    HormoneReceptorStatus, BreastGeneticExpression, BreastNormalizedAssay,
    SarcomaPathology, FNCLCCGrade, NecrosisPercentValue, NecrosisPercentRange,
    necrosis_percent_as_score
)
from . import xls
from . import parsers


class PathologyError(Exception):
    pass


def filter(filename, subject, collection):
    """
    Finds the pathology XLS row which matches the given subject.

    :param filename: the Excel workbook file location
    :param subject: the XNAT subject name
    :param collection: the image collection name
    :return: the pathology :meth:`qipipe.qiprofile.xls.filter` rows list
    """
    factory = Factory.for_collection(collection)
    reader = xls.Reader(filename, 'Pathology', **factory.parsers)
    return list(reader.filter(subject))


def update(subject, rows):
    """
    Updates the given subject data object from the pathology XLS rows.
    Each pathology XLS row subsumes both a biopsy encounter data object
    and its embedded pathology data object. The only biopsy attribute is
    the date. The date is used to match, or, if necessary, create
    a Biopsy object for that date. The factory creates a new pathology
    object. For an existing biopsy, this new pathology object replaces
    the biopsy pathology. For a new biopsy, the biopsy pathology attribute
    is set to the new pathology object.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param rows: the pathology :meth:`filter` rows list

    """
    # The pathology object factory.
    factory = Factory.for_collection(subject.collection)
    for row in rows:
        # The existing or new biopsy encounter object.
        biopsy = _biopsy_for(row.date)
        # Set the biopsy pathology to a new pathology object.
        biopsy.pathology = factory.create(row)


def _biopsy_for(subject, date):
    """
    :param subject: the target `subject database object
    :param date: the biopsy date
    :return: the matching biopsy, or a new biopsy object if no match
        was found
    """
    # Look for an existing biopsy.
    target = next((enc for enc in subject.encounters
                   if isinstance(enc, Biopsy) and enc.date == date),
                  None)
    # If no match, then make a new biopsy database object.
    if not target:
        target = Biopsy(date=date)
        subject.encounters.append(target)

    # Return the existing or new biopsy object.
    return target


def _update(subject, row):
    """
    Updates the given subject data object from the pathology input.

    :param subject: the ``Subject`` Mongo Engine database object
        to update
    :param row: the input pathology :meth:`update` row
    :raise PathologyError: if the row dates are not contained within
        exactly one biopsy
    """
    # The pathology XLS row subsumes the Biopsy encounter and the
    # the biopsy pathology reference. The only Biopsy attribute is
    # the date. The date is used to match, or, if necessary, create
    # a Biopsy object with that date.
    tgt_date = row.date
    # Look for an existing biopsy.
    target = next((enc for enc in subject.encounters
                   if isinstance(enc, Biopsy) and enc.date == tgt_date),
                  None)
    # If no match, then make a new biopsy database object.
    if not target:
        biopsy = Biopsy(date=tgt_date)
        subject.encounters.append(biopsy)

    # Make the pathology object.
    biopsy.pathology = factory.create_pathology()


def _parse_tumor_size(value):
    """
    :param value: the input string or integer value
    :return: the TNM tumor size database object
    """
    if isinstance(value, six.string_types):
        return TNM.Size.parse(value)
    elif isinstance(value, int):
        return TNM.Size(tumor_size=value)
    else:
        raise PathologyError("The TNM Size value type is not supported:"
                              " %s %s" % (value, value.__class__))


class Factory(object):
    """Pathology data object factory."""

    PARSERS = dict(
        tumor_size=_parse_tumor_size
    )
    """The TNM XLS value parsers common to all tumor types."""

    @classmethod
    def for_collection(klass, collection):
        """
        :param klass: this :class:`Factory` class
        :param collection: the target image collection name
        :return: the factory class
        :raise PathologyError: if there is no factory class for the given
            collection
        """
        # The factory is the subclass with the same name as the collection.
        factory = next((k for k in klass.__subclasses__()
                        if k.__name__ == collection),
                       None)
        if not factory:
            raise PathologyError("Collection not supported: %s" % collection)

        return factory()

    def __init__(self, **opts):
        """
        :param opts: the Factory subclass-specific parsers
        """
        popts = parsers.default_parsers(TNM)
        popts.update(Factory.PARSERS)
        popts.update(opts)
        self.parsers = popts
        """The XLS attribute value parsers."""

    def tumor_type(self):
        """
        The required tumor type is a TNM attribute.

        :raise PathologyError: always, since tumor_type is a subclass
            responsibility
        """
        raise PathologyError("The tumor_type class method is a subclass"
                             " responsibility")

    def create(self, row):
        """
        :param row: the input XLS row
        :return: the new pathology data object
        :raise PathologyError: always, since this is a subclass
            responsibility
        """
        raise PathologyError("Pathology object creation is a subclass"
                             " responsibility")


    def create_tnm(self, row, grade):
        """
        :param row: the input XLS row
        :param grade: the tumor type-specific TNM grade
        :return: the new TNM database object
        """
        # The TNM {attribute: value} dictionary.
        raw = dict(grade=grade)
        raw_row = (row[attr] for attr in TNM._fields if attr in row)
        raw.update(raw_row)
        values = {k: v for k, v in raw.iteritems() if v != None}

        # Make the new TNM database object.
        return TNM(**values) if values else None


class Breast(Factory):
    """Breast pathology data object factory."""

    PARSERS = dict(
    
    
    
        ## TODO - the prefixed XLS hormone attributes, e.g. estrogen_positive,
        ## correspond to the respective data model HormoneReceptorStatus
        ## attributes. Handle this in both parsing and update.
    
    
    
    )
    """The non-default breast pathology XLS value parsers."""

    def __init__(self):


        
        ## TODO - add the default parsers for BreastGeneticExpression and HormoneReceptorStatus.
        ## TODO - add all CVs, incl. Sarcoma histology, to the spreadsheet column drop-downs.


        
        super(Breast, self).__init__()

    def tumor_type(self):
        """
        The required :meth:`Factory.tumor_type` tumor type.

        :return: ``Breast``
        """
        return 'Breast'

    def create(self, row):
        """
        :param row: the input XLS row
        :return: the ``BreastPathology`` data object
        """
        grade = self._create_grade(row)
        tnm = self.create_tnm(row, grade)
        # Make the hormone receptor status.
        hormone_receptors = self._create_hormone_receptors(row)
        # Make the genetic expression results.
        genetic_expression = self._create_genetic_expression(row)
        raw = dict(tnm=tnm, hormone_receptors=hormone_receptors,
                      genetic_expression=genetic_expression)
        values = {k: v for k, v in raw.iteritems() if v != None}
        
        # Return the new pathlogy database object.
        return BreastPathology(**values) if values else None

    def _create_grade(self, row):
        pass

    def _create_hormone_receptors(self, row):
        pass

    def _create_genetic_expression(self, row):
        pass


def _parse_necrosis_percent(s):
    """
    :param s: the input XLS string value
    :return: the necrosis percent database object
    """
    if not s:
        return None
    values = [int(s) for v in s.split('-')]
    if len(values) == 1:
        return NecrosisPercentValue(value=values[0])
    else:
        start_val, stop_val = values
        start_bnd = NecrosisPercentRange.LowerBound(value=start_val)
        stop_bnd = NecrosisPercentRange.UpperBound(value=stop_val)
        return NecrosisPercentRange(start=start_bnd, stop=stop_bnd)


class Sarcoma(Factory):
    """Sarcoma pathology data object factory."""

    PARSERS = dict(
        necrosis_percent=_parse_necrosis_percent
    )
    """The XLS value parsers specific to sarcoma tumors."""

    def __init__(self):
        super(Sarcoma, self).__init__(**Sarcoma.PARSERS)

    def tumor_type(self):
        """
        The required :meth:`Factory.tumor_type` tumor type.

        :return: ``Sarcoma``
        """
        return 'Sarcoma'

    def create(self, row):
        """
        :param row: the input XLS row
        :return: the ``SarcomaPathology`` database object
        """
        grade = self._create_grade(row)
        tnm = self.create_tnm(row, grade)
        raw = dict(tnm=tnm, location=row.location,
                      necrosis_percent=row.necrosis_percent,
                      histology=row.histology)
        values = {k: v for k, v in raw.iteritems() if v != None}

        # Make the new pathlogy database object.
        return SarcomaPathology(**values) if values else None

    def _create_grade(self, row):
        # Calculate the necrosis score from the necrosis percent.
        necrosis_score = necrosis_percent_as_score(row.necrosis_percent)
        raw = dict(differentiation=row.differentiation,
                   mitotic_count=row.mitotic_count,
                   necrosis=necrosis_score)
        values = {k: v for k, v in raw.iteritems() if v != None}
        
        return FNCLCCGrade(**values) if values else None
