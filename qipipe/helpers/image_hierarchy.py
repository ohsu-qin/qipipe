from collections import defaultdict
from .dictionary_hierarchy import DictionaryHierarchy
from .dicom_tag_reader import DicomTagReader

class ImageHierarchy(DictionaryHierarchy):
    """
    ImageHierarchy wraps the DICOM image patient-study-series-image hierarchy.
    """
    def __init__(self):
        # the patient: series: image nested dictionary
        self.tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        super(ImageHierarchy, self).__init__(self.tree)
        self._tag_reader = DicomTagReader('PatientID', 'StudyID', 'SeriesNumber', 'InstanceNumber')
    
    def add(self, ds):
        """
        Adds the patient-study-series-image hierarchy entries from the given DICOM dataset.

        @param ds: the DICOM dataset
        """
        # build the image hierarchy
        path = self._tag_reader.read(ds)
        self.tree[path[0]][path[1]][path[2]].append(path[3])
