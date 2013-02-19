import sys
import os
import re
import glob
from dicom import datadict as dd
from .dicom_helper import iter_dicom

import logging
logger = logging.getLogger(__name__)

# Turn off pydicom debugging.
import dicom
dicom.debug(False)

def edit_dicom_headers(source, dest, tag_values):
    """
    Sets the tags of the DICOM files in the given input directory.
    
    The modified DICOM dataset is written to a file in the destination directory.
    The source path structure is preserved, e.g. if the source is
    C{/path/to/source} and the destination is
    C{/path/to/destination}, then the edited C{/path/to/source/patient04/visit01/image0004.dcm}
    is written to C{/path/to/destination/patient04/visit01/image0004.dcm}.
    
    @param source: the directory containing the input DICOM files
    @param dest: the directory in which to write the modified DICOM files
    @param tag_values: the DICOM header {name: value} tag values to set
    @return: the files which were created
    """
    
    # The {tag: value} dictionary.
    tv = {dd.tag_for_name(t.replace(' ', '')): v for t, v in tag_values.iteritems()}
    # The {tag: VR} dictionary.
    tvr = {t: dd.get_entry(t)[0] for t in tv.iterkeys()}
    logger.info("Editing the %(source)s DICOM files with the following tag values: %(tv)s..." % {'source' : source, 'tv': tag_values})
    files = []
    for ds in iter_dicom(source):
        for  t, v in tv.iteritems():
            try:
                ds[t].value = v
            except KeyError:
                ds.add_new(t, tvr[t], v)
        # Write the modified dataset to the output file.
        rel_path = ds.filename[(len(source) + 1):]
        fname = os.path.join(dest, rel_path)
        d = os.path.dirname(fname)
        if not os.path.exists(d):
            os.makedirs(d)
        ds.save_as(fname)
        files.append(fname)
        logger.debug("Saved the edited DICOM file %(src)s as %(tgt)s." % {'src': ds.filename, 'tgt': fname})
    logger.info("The edited %(source)s DICOM files were saved in %(dest)s." % {'source' : source, 'dest': dest})
    return files
