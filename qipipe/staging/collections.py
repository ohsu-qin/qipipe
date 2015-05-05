from .staging_error import StagingError

extent = {}
"""The {name: collection} dictionary."""

def add(*collections):
    """
    Adds the given :class:`qipipe.staging.collection.Collection`s to the
    list of known collections.
    
    :param collections: the collection objects to add
    """
    for coll in collections:
        extent[coll.name] = coll


def with_name(name):
    """
    :param name: the image collection name
    :return: the corresponding :class:`qipipe.staging.collection.Collection`
    :raise StagingError: if the collection is not recognized
    """
    coll = extent.get(name, None)
    if not coll:
        raise StagingError("The AIRC collection name is not recognized: %s" %
                           name)

    return coll
