import re
from datetime import datetime

TRAILING_NUM_REGEX = re.compile("(\d+)$")
"""A regular expression to extract the trailing number from a string."""

DATE_REGEX = re.compile("(0?\d|1[12])/(0?\d|[12]\d|3[12])/((19|20)?\d\d)$")


class DateError(Exception):
    pass


def trailing_number(s):
    """
    :param s: the input string
    :return: the trailing number in the string, or None if there
        is none 
    """
    match = TRAILING_NUM_REGEX.search(s)
    if match:
        return int(match.group(1))


def default_parser(attribute):
    """
    Retuns the default parser, determined as follows:
    * If the attribute ends in ``date``, then a MM/DD/YYYY datetime parser
    
    :param attribute: the row attribute
    :return: the value parser function, or None if none
    """
    if attribute.endswith('date'):
        return _parse_date


def _parse_date(s):
    """
    :param s: the input date string
    :return: the parsed datetime
    :rtype: datetime
    """
    match = DATE_REGEX.match(s)
    if not match:
        raise DateError("Date is not in a supported format: %s" % s)
    m, d, y = map(int, match.groups()[:3])
    if y < 20:
        y += 2000
    elif y < 100:
        y += 1900
    
    return datetime(y, m, d)

