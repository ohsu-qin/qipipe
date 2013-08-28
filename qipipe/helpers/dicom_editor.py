import sys
import os
import re
import glob
from dicom import datadict as dd
from .dicom_helper import iter_dicom

from .logging_helper import logger


# Turn off pydicom debugging.
import dicom
dicom.debug(False)

def edit_dicom_headers(dest, *dicom_files, **tag_values):
    """
    Sets the tags of the given DICOM files.
    
    :param dest: the directory in which to write the modified DICOM files
    :param dicom_files: the files or directories containing the input DICOM files
    :param tag_values: the DICOM header (I{name}, I{value}) tag values to set
    :return: the files which were created
    """
    dest = os.path.abspath(dest)
    if not os.path.exists(dest):
        os.makedirs(dest)
    # The {tag: value} dictionary.
    tv = {dd.tag_for_name(t.replace(' ', '')): v for t, v in tag_values.iteritems()}
    # The {tag: VR} dictionary.
    tvr = {t: dd.get_entry(t)[0] for t in tv.iterkeys()}
    files = []
    
    logger(__name__).info("Editing the DICOM files with the following tag values: %s..." % tag_values)
    # Open the DICOM store on each DICOM file (skipping non-DICOM files),
    # set the tag values and save to a new file in the destination directory.
    for ds in iter_dicom(*dicom_files):
        for  t, v in tv.iteritems():
            if t in ds:
                ds[t].value = v
            else:
                ds.add_new(t, tvr[t], v)
        # Write the modified dataset to the output file.
        _, fname = os.path.split(ds.filename)
        out_file = os.path.join(dest, fname)
        ds.save_as(out_file)
        files.append(out_file)
        logger(__name__).debug("Saved the edited DICOM file as %s." % out_file)
    logger(__name__).info("The edited DICOM files were saved in %s." % dest)
    
    return files
