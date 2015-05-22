# Absolute import (the default in a future Python release) resolves
# the csv import as the standard Python csv module rather
# than this module of the same name.
from __future__ import absolute_import


import re
import csv
from contextlib import contextmanager
from datetime import datetime
from bunch import Bunch
import inflection
from qiutil.logging import logger
from .helpers import trailing_number

IDENTITY = lambda v: v
"""A function returning the parameter."""


class CSVError(Exception):
    pass


@contextmanager
def read(in_file, parser=None):
    # Note: open the file in universal newline mode ('U') to
    # accomodate Mac newlines.
    with open(self.file, 'rU') as csvfile:
        yield Reader(csvfile, parser)


class Reader(object):
    """Reads a clinical CSV file stream."""
    
    def __init__(self, csvfile, parser=None):
        """
        :param csvfile: the input CSV file stream
        :param parser: the attribute value parser generator
        """
        self.reader = csv.DictReader(csvfile)
        """The input CSV row reader."""
        
        self._field_attr_dict = {fld: self._attributize(fld)
                                for fld in reader.fieldnames}
        """The {field: attribute} dictionary."""
        
        @property
        def attributes(self):
            """
            :return: the lower-case, underscore field names.
            """
            return self._field_attr_dict.itervalues()
        
        if not parser:
            parser = lambda attr: None
        self._parsers = {attr: parser(attr) for attr in self.attributes}
        """The {attribute: parser} dictionary."""

    def __iter__(self):
        return self.next()

    def next(self):
        """
        Converts each input CSV field into a lower-case, underscore
        attribute and parses each input CSV row field value.
        
        :yield: the CSV row {attribute: parsed value} dictionary
        """
        # Format each row.
        for in_row in self.reader:
            # The {attribute: input value} row dictionary.
            attr_csv_dict = {self._field_attr_dict[k]: v
                             for k, v in in_row.iteritems()}
            # Yield the {attribute: parsed value} row dictionary.
            yield {k: self._format(k, v, self._parsers[k])
                   for k, v in attr_csv_dict.iteritems()}

    def filter(self, subject, session=None):
        """
        :param subject: the XNAT subject name
        :param session: the XNAT session name (required only if the
            session field is in the file)
        :yield: the :meth:`next` row
        """
        if not subject:
            raise CSVError("The CSV reader subject is missing")
        self.subject = trailing_number(subject)
        """The target subject number."""
        
        if session:
            self.session = trailing_number(session)
            """The target session number."""

        # The required subject field.
        sbj_fld = next((fld for fld, attr in self._field_attr_dict.iteritems()
                        if attr == 'subject' or attr == 'patient'), None)
        if not sbj_fld:
            raise CSVError("CSV file does not have a subject or patient"
                           " field in the %s CSV file headers: %s" %
                           (self.file, reader.fieldnames))

        # The optional session field.
        if self.session:
            sess_fld = next((fld for fld, attr in self._field_attr_dict.iteritems()
                             if attr == 'session' or attr == 'visit'), None)
            if not sess_fld:
                raise CSVError("CSV file does not have a session or visit"
                               " field in the %s CSV file headers: %s" %
                               (self.file, reader.fieldnames))
        
        # Apply the filter to each row.
        for row in self:
            # Match on the subject number.
            row_sbj = row.pop('subject')
            row_sbj_nbr = trailing_number(row_sbj)
            # Check the row subject number against the target.
            if row_sbj_nbr != self.subject:
                # No match: skip this row.
                continue
            # The row subject number matches the target. If there is a
            # session target, then match on the session number as well.
            if self.session:
                row_sess = row.pop('session')
                row_sess_nbr = trailing_number(row_sess)
                if row_sess_nbr != self.session:
                    # No match; skip this row.
                    continue
            
            # We have a winner. Make a bunch with the subject number.
            matched = Bunch(subject=self.subject)
            # If the row has a session, then set the session number
            # attribute.
            if self.session:
                matched.session = self.session
            # Add the remaining fields as attributes.
            matched.update(row)

            # Yield the row bunch.
            yield row
            
    def _format(self, attribute, value, parser):
        return None if value == None or value == '' else parser(value)

    def _attributize(self, s):
        """
        Converts the given CSV field name to an underscore attribute.
        
        :param s: the string to convert
        :return: the underscore attribute name
        """
        return inflection.underscore(re.sub(r'\W+', '_', s))
