from .collection_helper import is_nonstring_iterable

def on(root):
    """
    @param root: the nested dictionary to wrap
    @return: a new DictionaryHierarchy on the given root dictionary
    """
    return DictionaryHierarchy(root)

class DictionaryHierarchy(object):
    """
    A DictionaryHierarchy wraps a nested dictionary. The DictionaryHierarchy iterator
    enumerates the root -> leaf paths.
    
    For example, the hierarchy of a nested dictionary given by::
    
        1 : '1'
        2 : 
          3 : '3'
          4 : '4', '5'
          6 :
            7 : '7'
        8 : '8'
    
    results in the following paths::
    
        1, '1'
        2, 3, '3'
        2, 4, '4'
        2, 4, '5'
        2, 6, 7, '7'
        8, '8'
    """
    def __init__(self, root):
        """
        @param root: the nested dictionary to wrap by this hierarchy
        @type root: dict
        @raise ArgumentError: if the given root is not a dictionary 
        """
        if not isinstance(root, dict):
            raise TypeError("The dictionary hierarchy root is not a dictionary: {1}" % root)
        self.root = root
    
    def __iter__(self):
        return self.Iterator(self.root)
    
    class Iterator(object):
        def __init__(self, root):
            """
            @param root: the nested dictionary to iterate over
            @type root: dict
            @raise ArgumentError: if the given root is not a dictionary 
            """
            self.base = root.iteritems()
            self.child = self.path = None

        def __iter__(self):
            return self
        
        def next(self):
            """
            Returns the next root-to-leaf path, determined as follows:
                - The first path member is the the current key in the wrapped root (key, value) iteration.
                - If the value is a dictionary, then recursively iterate over path tails given by that
                  dictionary's hierarchy.
                - Otherwise, if the value is a non-string iterable, then recursively iterate over
                  path tails given by that value's iteration.
                - Otherwise, the last member in the path is the value.
        
            @return: the next path
            @rtype: list 
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
            if is_nonstring_iterable(value):
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
