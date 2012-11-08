import sys
import os
import re
import glob
from ..helpers.dicom_tags import read_tags, InvalidDicomError

class StagingError(Exception):
    pass
    
def link_dicom_files(*args):
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
    Staging(opts).link_dicom_files(*args)

class Staging:
    def __init__(self, opts={}):
        """
        Creates a new Staging helper.
        
        @param opts: the staging options:
            target: the target directory in which to place the links (default is .)
            delta: the delta directory in which to place only the new links (default is None)
            include: the DICOM file include pattern (default is *)
            visit: the visit directory pattern (default is [Vv]isit*)
            verbosity: the verbosity level 'Warn', 'Error', 'Info' or None (default is Warn)
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
        # The message level.
        self.verbosity = opts.get('verbosity') or 'Warn'
        # The replace option.
        self.replace = opts.has_key('replace'):
     
    def link_dicom_files(self, *dirs):
        """
        Creates symbolic links in to the DICOM files in the given source patient directories.
        The patient/visit subdirectories are created in the target directory. The
        patient directory name is patient<nn>, where <nn> is the two-digit patient name, e.g.
        patient08. The visit directory name is visit<nn>, where <nn> is the two-digit patient
        visit number, e.g. visit02.
    
        If the delta option is given, then a link is created from the delta directory to each
        new patient visit directory, e.g. ./delta/patient08/visit02 -> ./patient08/visit02.
    
        @param dirs: the source patient directories 
        """
        # Build the staging area for each patient.  
        for d in dirs:
            self._stage(d)
    
    def _stage(self, path):
        """
        Creates the patient/visit staging area in the current working directory.
        Each visit subdirectory links the DICOM files in the given source patient
        directories.
        
        See link_dicom_files.
        
        @param path: the source patient directory path
        """
        src_pnt_dir = os.path.normpath(path)
        # The RE to extract the patient or visit number suffix.
        npat = re.compile('\d+$')
        # Extract the patient number from the patient directory name.
        pnt_match = npat.search(os.path.basename(src_pnt_dir))
        if not pnt_match:
            raise StagingError('The source patient directory does not end in a number: ' + src_pnt_dir)
        pnt_nbr = int(pnt_match.group(0))
        # The patient directory which will hold the visits.
        tgt_pnt_dir_name = "patient%02d" % pnt_nbr
        tgt_pnt_dir = os.path.join(self.tgt_dir, tgt_pnt_dir_name)
        if not os.path.exists(tgt_pnt_dir):
            os.makedirs(tgt_pnt_dir)
        # Build the target visit subdirectories.
        for src_visit_dir in glob.glob(os.path.join(src_pnt_dir, self.vpat)):
            # Extract the visit number from the visit directory name.
            visit_match = npat.search(os.path.basename(src_visit_dir))
            if not visit_match:
                raise StagingError('The source visit directory does not end in a number: ' + src_visit_dir)
            visit_nbr = int(visit_match.group(0))
            # The visit directory which holds the links to the source files.
            tgt_visit_dir_name = "visit%02d" % visit_nbr
            tgt_visit_dir = os.path.join(tgt_pnt_dir, tgt_visit_dir_name)
            # Skip an existing visit.
            if os.path.exists(tgt_visit_dir):
                if not self.replace:
                    if self.verbosity:
                        print "Skipped existing visit directory %s." % tgt_visit_dir
                    continue
            else:
                # Make the target visit directory.
                os.mkdir(tgt_visit_dir)
            # Link the delta visit directory to the target, if necessary.
            if self.delta_dir:
                delta_pnt_dir = os.path.join(self.delta_dir, tgt_pnt_dir_name)
                if not os.path.exists(delta_pnt_dir):
                    os.makedirs(delta_pnt_dir)
                delta_visit_dir = os.path.join(delta_pnt_dir, tgt_visit_dir_name)
                os.symlink(os.path.relpath(tgt_visit_dir, delta_pnt_dir), delta_visit_dir)
            # Link each of the DICOM files in the source concatenated subdirectories.
            for src_file in glob.glob(os.path.join(src_visit_dir, self.include)):
                if os.path.isdir(src_file):
                    if self.verbosity:
                        print >> "Skipped directory %s." % src_file
                        continue
                # Check whether the file has a DICOM header
                try:
                    read_tags(src_file)
                except InvalidDicomError:
                    if self.verbosity:
                        print >> "Skipped non-DICOM file %s." % src_file
                else:
                    tgt_file_base = os.path.basename(src_file).replace(' ', '_')
                    # Replace blanks in the file name.
                    tgt_name, tgt_ext = os.path.splitext(tgt_file_base)
                    # The file extension should be .dcm .
                    if tgt_ext != '.dcm':
                        tgt_file_base = tgt_name + '.dcm'
                    # Link the source DICOM file.
                    os.symlink(src_file, os.path.join(tgt_visit_dir, tgt_file_base))
