import os
import inspect

class FileIteratorError(Exception):
    pass

class FileIterator(object):
    """
    This FileIterator iterates over the files contained in the given file specifications.
    """
    def __init__(self, *filespecs):
        """
        @param filespecs: the files, directories or file generators over which to iterate
        """
        self._filespecs = filespecs
    
    def __iter__(self):
        return self.next()
        
    def next(self):
        """
        Yields the next file as follows:
            - If the current file specification is a file, then yield that file.
            - If the current file specification is a directory, then yield each file
              contained in that directory.
            - If the current file specification is a generator, then yield each
              generated item 
        """
        for spec in self._filespecs:
            if isinstance(spec, str):
                if os.path.isfile(spec):
                    yield spec
                elif os.path.isdir(spec):
                    for root, dirs, fnames in os.walk(spec):
                        for fn in fnames:
                            yield os.path.join(root, fn)
                else:
                    raise FileIteratorError("File not found: %s" % spec)
            elif inspect.isgenerator(spec):
                for f in spec:
                    yield f
            else:
                raise FileIteratorError("File iteration item is not supported: %s" % spec.__class__)
        