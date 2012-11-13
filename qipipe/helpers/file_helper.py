import os

class FileIterator(object):
    """
    This FileIterator iterates over the files contained in one or more directories.
    """
    def __init__(self, *files):
        """
        @param files: the file or directory paths.
        """
        self._files = files
    
    def __iter__(self):
        return self.next()
        
    def next(self):
        for f in self._files:
            if os.path.isfile(f):
                yield f
            elif os.path.isdir(f):
                for root, dirs, files in os.walk(f):
                    for f in files:
                        path = os.path.join(root, f)
                        yield path
