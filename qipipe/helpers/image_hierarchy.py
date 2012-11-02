from collections import defaultdict
from .dictionary_hierarchy import DictionaryHierarchy

class ImageHierarchy(DictionaryHierarchy):
    """
    ImageHierarchy wraps the DICOM image patient-study-series-image hierarchy.
    """
    def __init__(self):
        # the patient: series: image nested dictionary
        self.tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        DictionaryHierarchy.__init__(self, self.tree)
    
    def add(self, ds):
        """
        Adds the patient-study-series-image hierarchy entries from the given DICOM dataset.

        @param ds: the DICOM dataset
        """
        # build the image hierarchy
        path = self._path(ds)
        self.tree[path[0]][path[1]][path[2]].append(path[3])

    def _path(self, ds):
        """
        @param ds: the DICOM dataset
        @return: the patient, study, series and image tags
        @rtype: list
        """
        return [str(tag) for tag in [ds.PatientID, ds.StudyID, ds.SeriesNumber, ds.InstanceNumber]]