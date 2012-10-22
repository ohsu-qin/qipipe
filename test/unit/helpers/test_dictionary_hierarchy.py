import unittest
from collections import OrderedDict
from qipipe.helpers import dictionary_hierarchy as hierarchy

class TestHierarchy(unittest.TestCase):
    """DictionaryHierarchy unit tests."""
    
    def test_flat(self):
         self.assertEqual([[1, '1']], list(hierarchy.on({1: '1'})))
        
    def test_nested(self):
        """
        Tests that the hierarchy of a nested dictionary given by:
        
        1 : '1'
        2 : 
          3 : '3'
          4 : '4', '5'
          6 :
            7 : '7'
        8 : '8'
        
        results in the follwing paths:
        
        1, '1'
        2, 3, '3'
        2, 4, '4'
        2, 4, '5'
        2, 6, 7, '7'
        8, '8'
        """
        d = OrderedDict([[1, '1'], [2, OrderedDict([ [3, '3'], [4, ['4', '5']], [6, {7: '7'}] ])], [8, ['8']] ])
        self.assertEqual([[1, '1'], [2, 3, '3'], [2, 4, '4'], [2, 4, '5'], [2, 6, 7, '7'], [8, '8']], list(hierarchy.on(d)))

if __name__ == "__main__":
    unittest.main()
