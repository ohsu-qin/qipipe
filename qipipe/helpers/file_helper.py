import os

class FileIterator(object):
    """
    This FileIterator iterates over the paths contained in one or more directories.
    """
    def __init__(self, *paths):
        """
        @param paths: the file or directory paths.
        """
        self._paths = paths
    
    def __iter__(self):
        return self.next()
        
    def next(self):
        for f in self._paths:
            if os.path.isfile(f):
                yield f
            elif os.path.isdir(f):
                for root, dirs, paths in os.walk(f):
                    for f in paths:
                        path = os.path.join(root, f)
                        yield path
