from .. import *

class TestHierarchyIterator(unittest.TestCase):
    """HierarchyIterator unit tests."""
    
    # def test_dictionary(self):
    #     HierarchyIterator
    
    def test_none(self):
        self.assertEqual(HierarchyIterator(None).next(), [], '')

if __name__ == "__main__":
    unittest.main()
