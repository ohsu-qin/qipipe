from .collection_helper import is_nonstring_collection

class DictionaryHierarchy:
    """
    A DictionaryHierarchy wraps a nested dictionary.
    """
    def __init__(self, root):
        """
        :param root: the nested dictionary to wrap by this hierarchy
        :type root: dict
        :raise: ArgumentError if the given root is not a dictionary 
        """
        if not isinstance(root, dict):
            raise TypeError("The dictionary hierarchy root is not a dictionary: {1}" % root)
        self.root = root
    
    def __iter__(self):
        return self.Iterator(self.root)
    
    class Iterator:
        def __init__(self, root):
            """
            :param base: the nested dictionary to iterate over
            :type root: dict
            :raise: ArgumentError if the given root is not a dictionary 
            """
            self.base = root.iteritems()
            self.child = self.path = None

        def __iter__(self):
            return self
        
        def next(self):
            """
            Returns the next root-to-leaf path, determined as follows:
            * The first path member is the the current key in the wrapped root (key, value) iteration.
            * If the value is a dictionary, then recursively iterate over path tails given by that
              dictionary's hierarchy.
            * Otherwise, if the value is a non-string iterable, then recursively iterate over
              path tails given by that value's iteration.
            * Otherwise, the last member in the path is the value.
        
            :return: the next path
            :rtype: list 
            """
            if self.child:
                try:
                    # Try to advance the child and replace the path tail.
                    self.path = self.path[:1] + self.child.next()
                    return self.path
                except StopIteration:
                    # Not an error; iterate to the next child.
                    self.child = None
        
            # Iterate to the next (key, value) pair.
            key, value = self.base.next()
            # The path begins with the key.
            self.path = [key]
            # If the next value is a non-string collection, then iterate to the child.
            # Otherwise, append the value to the path.
            if is_nonstring_collection(value):
                # Wrap a dictionary value as a child hierarchy.
                if isinstance(value, dict):
                    self.child = iter(DictionaryHierarchy(value))
                else:
                    self.child = iter([[item] for item in value])
                # Recursively iterate into the new child.
                return self.next()
            else:
                self.child = None
                # The path is the (key, value) pair.
                self.path.append(value)
                return self.path
