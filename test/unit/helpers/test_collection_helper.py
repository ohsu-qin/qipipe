import os
import glob
from nose.tools import (assert_equal, assert_raises, assert_true, assert_false)
from qipipe.helpers.collection_helper import *


class TestCollectionHelper:
    """dicom_helper unit tests."""

    def test_is_nonstring_iterable(self):
        assert_true(is_nonstring_iterable(['a', 'b']), "List is not recognized as a non-string collection")
        assert_false(is_nonstring_iterable('a'), "String is incorrectly recognized as a non-string collection")

    def test_to_series(self):
        assert_equal(to_series([1, 2, 3]), '1, 2 and 3', "Series formatter incorrect")
        assert_equal(to_series([1, 2, 3], 'or'), '1, 2 or 3', "Series formatter with conjunction incorrect")
        assert_equal(to_series([1]), '1', "Singleton series formatter incorrect")
        assert_equal(to_series([]), '', "Empty series formatter incorrect")
    
    def test_immutable_dict(self):
        idict = ImmutableDict(foo='bar')
        assert_equal(idict['foo'], 'bar', "Value was not set.")
        with assert_raises(NotImplementedError):
            idict['foo'] = 'baz'


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
