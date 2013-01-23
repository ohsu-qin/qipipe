import sys
import os
import re
import glob
from qipipe.helpers.logging import logger
from qipipe.helpers.dicom_helper import read_dicom_header, InvalidDicomError
from .staging_error import StagingError
    
def group_dicom_files(*args):
    """
    Links DICOM files in the given patient directories.
    
    @param args: the patient directories, optionally followed by the staging options
    """
    last = args[len(args) - 1]
    if isinstance(last, dict):
        args = args[:-1]
        opts = last
    else:
        opts = {}
    DICOMFileGrouper(opts).group_dicom_files(*args)

class DICOMFileGrouper(object):
    def __init__(self, opts={}):
        """
        Creates a new Staging helper.
        
        @param opts: the staging options:
            - target: the target directory in which to place the links (default is C{.})
            - delta: the delta directory in which to place only the new links (default is C{None})
            - include: the DICOM file include pattern (default is C{*})
            - visit: the visit directory pattern (default is C{[Vv]isit*})
        """
        # The target root directory.
        self.tgt_dir = opts.get('target') or '.'
        # The delta directory.
        if opts.get('delta'):
            self.delta_dir = os.path.abspath(opts['delta'])
        else:
            self.delta_dir = None
        # The file include pattern.
        self.include = opts.get('include') or '*'
        # The visit directory pattern.
        self.vpat = opts.get('visit') or '[Vv]isit*'
        # The replace option.
        self.replace = opts.has_key('replace')
     
    def group_dicom_files(self, *dirs):
        """
        Creates symbolic links to the DICOM files in the given source patient directories.
        The patient/visit/series subdirectories are created in the target directory. The
        patient directory name is C{patient}I{<nn>}, where I{<nn>} is the two-digit patient
        name, e.g. C{patient08}. The visit directory name is C{visit}I{<nn>}, where I{<nn>}
        is the two-digit patient visit number, e.g. C{visit02}. The series directory name
        is C{series}I{<nnn>}, where I{<nnn>} is the three-digit series number, e.g. C{series001}.
        
    
        If the delta option is given, then a link is created from the delta directory to each
        new patient visit directory, e.g.::
            
            ./delta/patient08/visit02 -> ./patient08/visit02
    
        @param dirs: the source patient directories 
        """
        # Build the staging area for each patient.  
        for d in dirs:
            self._group(d)
    
    def _group(self, path):
        """
        Creates the patient/visit staging area in the current working directory.
        Each visit subdirectory links the DICOM files in the given source patient
        directories.
        
        See group_dicom_files.
        
        @param path: the source patient directory path
        """
        src_pt_dir = os.path.abspath(path)
        # The RE to extract the patient or visit number suffix.
        npat = re.compile('\d+$')
        # Extract the patient number from the patient directory name.
        pt_match = npat.search(os.path.basename(src_pt_dir))
        if not pt_match:
            raise StagingError('The source patient directory does not end in a number: ' + src_pt_dir)
        pt_nbr = int(pt_match.group(0))
        # The patient directory which will hold the visits.
        tgt_pt_dir_name = "patient%02d" % pt_nbr
        tgt_pt_dir = os.path.abspath(os.path.join(self.tgt_dir, tgt_pt_dir_name))
        if not os.path.exists(tgt_pt_dir):
            os.makedirs(tgt_pt_dir)
            logger.info("Created patient directory %s." % tgt_pt_dir)
        # Build the target visit subdirectories.
        for src_visit_dir in glob.glob(os.path.join(src_pt_dir, self.vpat)):
            # Extract the visit number from the visit directory name.
            visit_match = npat.search(os.path.basename(src_visit_dir))
            if not visit_match:
                raise StagingError('The source visit directory does not end in a number: ' + src_visit_dir)
            visit_nbr = int(visit_match.group(0))
            # The visit directory which holds the links to the source files.
            tgt_visit_dir_name = "visit%02d" % visit_nbr
            tgt_visit_dir = os.path.join(tgt_pt_dir, tgt_visit_dir_name)
            # Skip an existing visit.
            if os.path.exists(tgt_visit_dir):
                if not self.replace:
                    logger.info("Skipped existing visit directory %s." % tgt_visit_dir)
                    continue
            else:
                # Make the target visit directory.
                os.mkdir(tgt_visit_dir)
                logger.info("Created visit directory %s." % tgt_visit_dir)
            # Link the delta visit directory to the target, if necessary.
            if self.delta_dir:
                delta_pt_dir = os.path.join(self.delta_dir, tgt_pt_dir_name)
                if not os.path.exists(delta_pt_dir):
                    os.makedirs(delta_pt_dir)
                delta_visit_dir = os.path.join(delta_pt_dir, tgt_visit_dir_name)
                if os.path.exists(delta_visit_dir):
                    os.remove(delta_visit_dir)
                # The delta link is relative to the target location.
                delta_rel_path = os.path.relpath(tgt_visit_dir, delta_pt_dir)
                os.symlink(delta_rel_path, delta_visit_dir)
                logger.info("Linked the delta visit directory {0} -> {1}.".format(delta_visit_dir, delta_rel_path))
            # Link each of the DICOM files in the source concatenated subdirectories.
            for src_file in glob.glob(os.path.join(src_visit_dir, self.include)):
                if os.path.isdir(src_file):
                    logger.info("Skipped directory %s." % src_file)
                    continue
                # If the file has a DICOM header, then get the series number.
                # Otherwise, skip the file.
                series = self._series_number(src_file)
                if series:
                    tgt_file_base = os.path.basename(src_file).replace(' ', '_')
                    # Replace blanks in the file name.
                    tgt_name, tgt_ext = os.path.splitext(tgt_file_base)
                    # The file extension should be .dcm .
                    if tgt_ext != '.dcm':
                        tgt_file_base = tgt_name + '.dcm'
                    # Make the series directory, if necessary.
                    tgt_series_dir = os.path.join(tgt_visit_dir, "series%03d" % series)
                    if not os.path.exists(tgt_series_dir):
                        os.mkdir(tgt_series_dir)
                    # Link the source DICOM file.
                    tgt_file = os.path.join(tgt_series_dir, tgt_file_base)
                    if os.path.islink(tgt_file):
                        if self.replace:
                            os.remove(tgt_file)
                        else:
                            logger.info("Skipped existing image link %s." % tgt_file)
                            continue
                    # Create a link from the target to the source.
                    os.symlink(src_file, tgt_file)
                    logger.info("Linked the image file {0} -> {1}".format(tgt_file, src_file))
                else:
                    logger.info("Skipped non-DICOM file %s." % src_file)
    
    def _series_number(self, path):
        try:
            return read_dicom_header(path).SeriesNumber
        except InvalidDicomError:
            return None
        