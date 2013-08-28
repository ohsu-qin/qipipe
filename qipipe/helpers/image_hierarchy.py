from collections import defaultdict
from .dictionary_hierarchy import DictionaryHierarchy
from .dicom_reader import (iter_dicom_headers, select_dicom_tags)

def read_image_hierarchy(*files):
    """
    Returns the ImageHierarchy for the DICOM files in the given locations.

    :param files: the files or directories to walk for DICOM files
    :return: the image hierarchy
    :rtype: :class:`qipipe.helpers.image_hierarchy.ImageHierarchy`
    """
    # Build the hierarchy dictionary.
    h = ImageHierarchy()
    for ds in iter_dicom_headers(*files):
        h.add(ds)
    return h

class ImageHierarchy(DictionaryHierarchy):
    TAGS = ('Patient ID', 'Study Instance UID', 'Series Instance UID',
        'Instance Number')
    
    """
    ImageHierarchy wraps the DICOM image subject-study-series-image hierarchy.
    """
    def __init__(self):
        # the subject: series: image nested dictionary
        self.tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        super(ImageHierarchy, self).__init__(self.tree)
    
    def add(self, ds):
        """
        Adds the subject-study-series-image hierarchy entries from the given
        DICOM dataset.

        :param ds: the DICOM dataset
        """
        # build the image hierarchy
        tdict = select_dicom_tags(ds, *ImageHierarchy.TAGS)
        path = [tdict[t] for t in ImageHierarchy.TAGS]
        self.tree[path[0]][path[1]][path[2]].append(path[3])
