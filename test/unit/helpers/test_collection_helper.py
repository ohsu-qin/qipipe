import os
import glob
from nose.tools import *

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from qipipe.helpers.collection_helper import *


class TestCollectionHelper:
    """dicom_helper unit tests."""

    def test_is_nonstring_iterable(self):
        assert_true(is_nonstring_iterable(['a', 'b']), "List is not recognized as a non-string collection")
        assert_false(is_nonstring_iterable('a'), "String is incorrectly recognized as a non-string collection")

    def test_to_series(self):
        assert_equal('1, 2 and 3', to_series([1, 2, 3]), "Series formatter incorrect")
        assert_equal('1, 2 or 3', to_series([1, 2, 3], 'or'), "Series formatter with conjunction incorrect")
        assert_equal('1', to_series([1]), "Singleton series formatter incorrect")
        assert_equal('', to_series([]), "Empty series formatter incorrect")
    
    def test_immutable_dict(self):
        idict = ImmutableDict(foo='bar')
        assert_equal('bar', idict['foo'], "Value was not set.")
        with assert_raises(NotImplementedError):
            idict['foo'] = 'baz'


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
