from collections import Iterable

def is_nonstring_iterable(obj):
    """
    @param obj: the object to check
    @return: whether the given object is a non-string iterable object
    """
    return isinstance(obj, Iterable) and not isinstance(obj, str)

def to_series(items, conjunction='and'):
    """
    Formats the given items as a series string, e.g.:
    
    to_series([1, 2, 3]) #=> '1, 2 and 3'
    
    @param items: the items to format in a series
    @param conjunction: the series conjunction
    @return: the items series
    @rtype: str
    """
    if not items:
        return ''
    prefix = ', '.join([str(i) for i in items[:-1]])
    suffix = str(items[-1])
    if not prefix:
        return suffix
    else:
        return (' ' + conjunction + ' ').join([prefix, suffix])