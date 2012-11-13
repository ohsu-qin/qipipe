import operator

class DicomTagReader(object):
    """
    DicomTagReader is a utility class for reading DICOM tag values.
    """
    
    def __init__(self, *tags):
        """
        @param tags: the tags to read
        """
        self._callers = [operator.attrgetter(tag.replace(' ', '')) for tag in tags]
        
    def read(self, ds):
        """
        @param ds: the pydicom dicom data set
        @yield: the value list for this reader's tags
        """
        return [str(rdr(ds)) for rdr in self._callers]
