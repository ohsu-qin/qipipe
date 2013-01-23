from collections import defaultdict
from .dictionary_hierarchy import DictionaryHierarchy
from .dicom_reader import iter_dicom_headers, select_dicom_tags

def read_image_hierarchy(*files):
    """
    Returns the ImageHierarchy for the DICOM files in the given locations.

    @param files: the files or directories to walk for DICOM files
    @return: the image hierarchy
    @rtype: ImageHierarchy
    """
    # Build the hierarchy dictionary.
    h = ImageHierarchy()
    for ds in iter_dicom_headers(*files):
        h.add(ds)
    return h

class ImageHierarchy(DictionaryHierarchy):
    TAGS = ('PatientID', 'Study Instance UID', 'Series Instance UID', 'InstanceNumber')
    
    """
    ImageHierarchy wraps the DICOM image patient-study-series-image hierarchy.
    """
    def __init__(self):
        # the patient: series: image nested dictionary
        self.tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        super(ImageHierarchy, self).__init__(self.tree)
    
    def add(self, ds):
        """
        Adds the patient-study-series-image hierarchy entries from the given DICOM dataset.

        @param ds: the DICOM dataset
        """
        # build the image hierarchy
        path = select_dicom_tags(ds, *TAGS)
        self.tree[path[0]][path[1]][path[2]].append(path[3])
