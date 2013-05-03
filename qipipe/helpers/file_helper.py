import os
import inspect
import base64
import struct
import time
import calendar

def generate_file_name(ext=None):
    """
    Makes a valid file name which is unique to within one millisecond of calling this function.
    
    @param: the optional file extension, with leading period delimiter
    @return: the file name
    """
    
    # A starting time prior to now.
    start = time.mktime(calendar.datetime.date(2013,01,01).timetuple())
    # A long which is unique to within one millisecond.
    offset = long((time.time() - start) * 1000)
    # The file name is encoded from the offset without trailing filler or linebreak.
    fname = base64.urlsafe_b64encode(struct.pack('L', offset)).rstrip('A=\n')
    
    if ext:
        return fname + ext
    else:
        return fname

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
            if isinstance(spec, file):
                yield spec
            elif isinstance(spec, str):
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
        