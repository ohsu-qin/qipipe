import re
import functools
import six
import numbers
import mongoengine
from qiutil import functions

TRAILING_NUM_REGEX = re.compile("(\d+)$")
"""A regular expression to extract the trailing number from a string."""

TRUE_REGEX = re.compile("(T(rue)?|Pos(itive)?|Present|Y(es)?)$", re.IGNORECASE)
"""
The valid True string representations are a case-insensitive match
for ``T(rue)?``, ``Pos(itive)?``, ``Present`` or ``Y(es)?``.
"""

FALSE_REGEX = re.compile("(F(alse)?|Neg(ative)?|Absent|N(o)?)$", re.IGNORECASE)
"""
The valid False string representations are a case-insensitive match
for ``F(alse)?``, ``Neg(ative)?``, ``Absent`` or ``N(o)?``.
"""

COMMA_DELIM_REGEX = re.compile(",\w*")
"""Match a comma with optional white space."""

TYPE_PARSERS = {
    mongoengine.fields.StringField: str,
    mongoengine.fields.IntField: int,
    mongoengine.fields.FloatField: float,
    # openpyxls dates are already the correct type.
    mongoengine.fields.DateTimeField: lambda v: v,
    # Wrap the functions below with a lambda as a convenience to allow
    # a forward reference to the parse functions defined below.
    mongoengine.fields.BooleanField: lambda v: parse_boolean(v),
    mongoengine.fields.ListField: lambda v: parse_list_string(v)
}
"""
The following type cast conversion parsers:
* string field => ``str``
* integer field => ``int``
* float field => ``float``
* boolean field => :meth:`parse_boolean`
* list field => :meth:`parse_list_string`
"""


class ParseError(Exception):
    pass


def parse_trailing_number(s):
    """
    :param s: the input string
    :return: the trailing number in the string
    :raise ParseError: if the input string does not have a trailing
        number
    """
    match = TRAILING_NUM_REGEX.search(s)
    if not match:
        raise ParseError("The input string does not have a trailing number:"
                         " %s" % s)
    
    return int(match.group(1))


def extract_trailing_number(value):
    """
    Returns the integer at the end of the given input value, as follows:
    * If the input value is an integer, then the result is the input
      value.
    * Otherwise, if the input value has a string type, then the result
      is the trailing digits converted to an integer.
    * Any other value is an error.
    :param the input integer or string
    :return: the trailing integer
    :raise ParseError: if the input value type is not int or a string type
    """
    if isinstance(value, numbers.Integral):
        return int(value)
    elif isinstance(value, six.string_types):
        return parse_trailing_number(value)
    else:
        raise ParseError("Cannot extract a trailing number from the value"
                         " %s of type %s" % (value, value.__class__))


def parse_list_string(value):
    """
    Converts a comma-separated list input string to a list, e.g.:
    
    >> from qipipe.qiprofile.parse import parse_list_string
    >> parse_list_string('White, Asian')
    ['White', 'Asian']
    
    :param value: the input value
    :return: the value converted to a list
    """
    if isinstance(value, six.string_types):
        return [word.strip() for word in COMMA_DELIM_REGEX.split(value)]
    else:
        return [value]


def parse_boolean(value):
    """
    Parses the input value as follows:
    * If the input value is already a boolean, then return the value
    * If the input is None or the empty string, then return None
    * Otherwise, if the input is a string which matches
        :const:`TRUE_REGEX`, then return True
    * Otherwise, if the input is a string which matches
        :const:`FALSE_REGEX`, then return False
    * Any other value is an error.
    
    :param value: the input value
    :return: the value as a boolean
    :raise ParseError: if the value cannot be converted
    """
    if isinstance(value, bool):
        return value
    elif not isinstance(value, six.string_types):
        raise ParseError("The input type cannot be converted to a boolean:"
                         " %s (%s)" % (value, value.__class__))
    elif not value:
        return None
    elif TRUE_REGEX.match(value):
        return True
    elif FALSE_REGEX.match(value):
        return False
    else:
        raise ParseError("The input value cannot be converted a boolean value:"
                         " %s" % value)


def default_parsers(*classes):
    """
    Associates the data model class fields to a parse function composed
    as follows:
    
    * The type cast function in :const:`TYPE_PARSERS`, if present
    * The controlled value lookup, if the field has controlled values
    
    :param classes: the data model classes
    :return: the {attribute: function} dictionary
    """
    parsers = {}
    for klass in classes:
        parsers.update(_default_parsers(klass))
    
    return parsers


def _default_parsers(klass):
    """
    :param klass: the data model class
    :return: the {attribute: function} dictionary
    """
    # The (attribute, parser or None) tuple generator.
    parsers = {}
    for attr, field in klass._fields.iteritems():
        parser = _default_parser(field)
        if parser:
            parsers[attr] = parser
    
    return parsers


def _default_parser(field):
    type_parser = _type_value_parser(field)
    cv_parser = _controlled_value_parser(field)
    # Compose CV look-up with type casting, allowing for
    # the possibility that one or both might be missing.
    parsers = [p for p in [cv_parser, type_parser] if p]
    
    return functions.compose(*parsers) if parsers else None


def _controlled_value_parser(field):
    """
    Associates the field to :meth:`controlled_value_for`
    if the field has controlled values.
    
    :param field: the data model field
    :return: the parser function, or None if this field does not have
        controlled values
    """
    if _has_controlled_values(field):
        return functools.partial(_controlled_value_for, field=field)


def _type_value_parser(field):
    """
    Returns the type cast conversion parser, as follows:
    * If the field has controlled value tuples, then convert the
      value to a string for the CV lookup
    * Otherwise, if the field has a :const:`TYPE_PARSERS` association,
      then use the associated parser
    * Ohterwise, no conversion is performed
    
    :param field: the data model field
    :return: the type cast conversion parser function
    """
    if field.choices and isinstance(field.choices[0], tuple):
        return str
    for field_type, parser in TYPE_PARSERS.iteritems():
        if isinstance(field, field_type):
            return parser


def _has_controlled_values(field):
    """
    :param field: the data model field
    :return: whether the field has controlled values
    """
    if isinstance(field, mongoengine.fields.ListField) and field.field:
        return _has_controlled_values(field.field)
    else:
        return field.choices != None


def _controlled_value_for(value, field):
    """
    Returns the controlled value which matches the given input value.
    The match is case-insensitive for strings.
    
    :param value: the input value
    :param field: the data model field object
    :return the matching controlled value
    :raise ParseError: if there is no match
    """
    # Recurse into a list.
    if isinstance(value, list):
        return [_controlled_value_for(v, field.field) for v in value]
    # The matching field choice.
    choice = _match_choice(value, field.choices)
    if choice == None:
        raise ParseError("The input %s value %s does not match one of"
                        " the supported field choices %s" %
                        (field, value, field.choices))
    
    # Return the controlled value specified by the choice.
    return _choice_controlled_value(choice)


def _match_choice(value, choices):
    for choice in choices:
        if _is_choice_match(value, choice):
            return choice


def _is_choice_match(value, choice):
    if isinstance(choice, tuple):
        return any((_is_choice_match(value, c) for c in choice))
    elif isinstance(choice, six.string_types):
        return str(value).lower() == choice.lower()
    else:
        return value == choice


def _choice_controlled_value(choice):
    return choice[0] if isinstance(choice, tuple) else choice
