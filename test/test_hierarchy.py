import os, sys
import unittest
import doctest
from collections import OrderedDict

sys.path.append(os.path.dirname(__file__) + '/../lib')

from helpers import hierarchy

class TestHierarchy(unittest.TestCase):
    """HierarchyIterator unit tests."""
    
    def test_list(self):
         self.assertEqual([1], hierarchy.on([1]).next())
    
    def test_dictionary(self):
         self.assertEqual([1, '1'], hierarchy.on({1: '1'}).next())
    
    def test_empty_list(self):
        self.assertFalse([i for i in hierarchy.on([])])
        
    def test_nested(self):
        d = OrderedDict([ [1, '1'], [2, OrderedDict([ [3, '3'], [4, ['4', '5']], [6, {7: '7'}] ])], [8, ['8']] ])
        self.assertEqual([[1, '1'], [2, 3, '3'], [2, 4, '4'], [2, 4, '5'], [2, 6, 7, '7'], [8, '8']], [i for i in hierarchy.on(d)])

if __name__ == "__main__":
    unittest.main()
