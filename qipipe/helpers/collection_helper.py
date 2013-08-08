from collections import Iterable

def is_nonstring_iterable(obj):
    """
    :param obj: the object to check
    :return: whether the given object is a non-string iterable object
    """
    return isinstance(obj, Iterable) and not isinstance(obj, str)

def to_series(items, conjunction='and'):
    """
    Formats the given items as a series string, e.g.:
    
    to_series([1, 2, 3]) #=> '1, 2 and 3'
    
    :param items: the items to format in a series
    :param conjunction: the series conjunction
    :return: the items series
    :rtype: str
    """
    if not items:
        return ''
    prefix = ', '.join([str(i) for i in items[:-1]])
    suffix = str(items[-1])
    if not prefix:
        return suffix
    else:
        return (' ' + conjunction + ' ').join([prefix, suffix])

class ImmutableDict(dict):
    """
    ImmutableDict is a dictionary that cannot be changed after creation.
    
    An ImmutableDict is *not* hashable and therefore cannot be used as a dictionary
    key or set member. See http://www.python.org/dev/peps/pep-0351 for the rationale.
    """
    
    def __int__(*args, **kwargs):
        super(ImmutableDict, self).__init__(*args, **kwargs)
    
    def __setitem__(self, key, value):
        """
        :raise NotImplementedError: always
        """
        raise NotImplementedError("The dictionary is immutable: %s" % self)

EMPTY_DICT = ImmutableDict()
"""
An immutable empty dictionary.
This constant serves as an efficient method return default value.
"""
