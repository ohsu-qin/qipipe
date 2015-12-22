import os
from nose.tools import (assert_equal, assert_is, assert_is_not_none)
from ... import ROOT
from qipipe.staging.image_collection import Collection
from qipipe.staging import image_collection
from ...helpers.logging import logger


class TestCollection(object):
    """Image collection unit tests."""
    
    def test_name(self):
        a = Collection('test', subject=None, session=None,
                       scan=None, volume=None)
        assert_equal(a.name, 'Test', "Collection name is not capitalized: %s" %
                                     a.name)
    
    def test_with_name(self):
        collections = {name: Collection(name, subject=None, session=None,
                                        scan=None, volume=None)
                       for name in ['Aa', 'bB']}
        for name, expected in collections.iteritems():
            actual = collection.with_name(name)
            assert_is_not_none(actual, "Collection search on exact name %s"
                                       " unsuccessful" % name)
            assert_is(actual, expected, "Collection search on exact name"
                                        " %s incorrect" % name)
            lc_name = name.lower()
            actual = collection.with_name(lc_name)
            assert_is_not_none(actual, "Collection search on lower-case name %s"
                                       " unsuccessful" % lc_name)
            assert_is(actual, expected, "Collection search on lower-case name"
                                        " %s incorrect" % lc_name)


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)