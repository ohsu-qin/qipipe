from collections import Iterable

def is_nonstring_collection(obj):
    """
    @param node: the object to check
    @return: whether the given object is a non-string iterable object
    """
    return isinstance(obj, Iterable) and not isinstance(obj, str)
