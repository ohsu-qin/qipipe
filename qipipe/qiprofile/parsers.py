import re
from functools import partial
import mongoengine
from qiutil import functions

TRAILING_NUM_REGEX = re.compile("(\d+)$")
"""A regular expression to extract the trailing number from a string."""

TRUE_REGEX = re.compile("(T(rue)?|Pos(itive)?|Y(es)?)$", re.IGNORECASE)
"""
The valid True string representations are a case-insensitive match
for ``T(rue)?``, ``Pos(itive)?`` or ``Y(es)?``.
"""

FALSE_REGEX = re.compile("(F(alse)?|Neg(ative)?|N(o)?)$", re.IGNORECASE)
"""
The valid False string representations are a case-insensitive match
for ``F(alse)?``, ``Neg(ative)?`` or ``N(o)?``.
"""

COMMA_DELIM_REGEX = re.compile(",\w*")
"""Match a comma with optional white space."""

TYPE_PARSERS = {
    mongoengine.fields.StringField: str,
    mongoengine.fields.IntField: int,
    mongoengine.fields.FloatField: float,
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


def parse_list_string(s):
    """
    Converts a comma-separated list input string to a list, e.g.:
    
    >> from qipipe.qiprofile import demographics
    >> demographics.PARSERS['races']('White, Asian')
    ['White', 'Asian']

    :param s: the input comma-separated list string
    :return: the string list
    """
    return [w.strip() for w in COMMA_DELIM_REGEX.split(s)]


def parse_boolean(s):
    """
    Parses the input string as follows:
    * If the input is None or the empty string, then None
    * Otherwise, if the input matches :const:`TRUE_REGEX`, then True
    * Otherwise, if the input matches :const:`FALSE_REGEX`, then False 
    * Any other value is an error.

    :param s: the input string
    :return: the value as a boolean
    :raise ParseError: if the string is invalid
    """
    if not s:
        return None
    elif TRUE_REGEX.match(s):
        return True
    elif FALSE_REGEX.match(s):
        return False
    else:
        raise ParseError("The string is not recognized as a boolean value:"
                           " %s" % s)


def default_parsers(klass):
    """
    Associates the data model class fields to :meth:`controlled_value_for`
    for those fields which have controlled values.
    
    :param klass: the data model class
    :return: the {attribute: function} dictionary
    """
    # The (attribute, parser or None) tuple generator.
    parsers = ((attr, _default_parser(field))
               for attr, field in klass._fields.iteritems())
    
    # Return the {attribute: parser} dictionary for only those
    # attributes which have a parser.
    return {attr: func for attr, func in parsers if func}


def _default_parser(field):
    cv_parser = _controlled_value_parser(field)
    type_parser = _type_value_parser(field)
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
        return partial(_controlled_value_for, field=field)


def _type_value_parser(field):
    """
    Returns the type cast conversion parser associated with the field
    type in :const:`TYPE_PARSERS`, or None if there is no match
    
    :param field: the data model field
    :return: the type cast conversion parser function
    """
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
        return not not field.choices


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
    elif isinstance(choice, str) or isinstance(choice, unicode):
        return str(value).lower() == choice.lower()
    else:
        return value == choice


def _choice_controlled_value(choice):
    return choice[0] if isinstance(choice, tuple) else choice
