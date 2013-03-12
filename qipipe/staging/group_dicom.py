import sys, os, re, glob
from qipipe.helpers.dicom_helper import read_dicom_header, InvalidDicomError
from .staging_error import StagingError

import logging
logger = logging.getLogger(__name__)


def group_dicom_files(collection, *dirs, **opts):
    """
    Groups AIRC input DICOM files by series.
    The AIRC directory structure is required to be in the form:

        I{subject}/I{session}/I{dicom}

    where I{subject}, I{session}, I{dicom} are the respective path glob
    patterns, as described in L{group_dicom_files}. The output is the
    grouped series directories in the form:

        I{collection}NN/C{session}NN/C{series}NNN

    where the series directory consists of links to the AIRC input
    DICOM files.

    Examples:

    For input DICOM files with directory structure:

        C{BreastChemo4/}
            CVisit1/}
                C{dce_concat/}
                    C{120210 ex B17_TT49}
                    ...

        GroupDicom(subject_dirs=[BreastChemo4], session_pat='Visit*', dicom_pat='*concat*/*', dest=data).run()

    results in output links with directory structure:

        C{Breast04/}
            C{session01/}
                C{series09/}
                    C{120210_ex_B17_TT49.dcm} -> C{BreastChemo4/Visit1/dce_concat/120210 ex B17_TT49}
                    ...
    
    @param dirs: the subject directories
    @param opts: the DICOMFileGrouper options
    @return: the target series directories which were added
    """
    return DICOMFileGrouper(collection, **opts).group_dicom_files(*dirs)


class DICOMFileGrouper(object):

    def __init__(self, collection, dest=None, delta=None, dicom_pat='*', session_pat='*', replace=False):
        """
        Creates a new DICOM file grouper.
        
        @param dest: the target directory in which to place the grouped subject directories (default is C{.})
        @param delta: the delta directory in which to place only the new links (default is C{None})
        @param dicom_pat: the DICOM file include pattern (default is C{*})
        @param session_pat: the session directory pattern (default is C{[Vv]isit*})
        """
        # The study collection.
        self.collection = collection
        # The target root directory.
        self.dest = dest or os.getcwd()
        # The delta directory.
        if delta:
            self.delta_dir = os.path.abspath(delta)
        else:
            self.delta_dir = None
        # The file include pattern.
        self.dicom_pat = dicom_pat
        # The session directory pattern.
        self.session_pat = session_pat
        # The replace option.
        self.replace = replace
     
    def group_dicom_files(self, *dirs):
        """
        Creates symbolic links to the DICOM files in the given source subject directories.
        The subject/session/series subdirectories are created in the target directory. The
        subject directory name is C{subject}I{<nn>}, where I{<nn>} is the two-digit subject
        name, e.g. C{subject08}. The session directory name is C{session}I{<nn>}, where I{<nn>}
        is the two-digit subject session number, e.g. C{session02}. The series directory name
        is C{series}I{<nnn>}, where I{<nnn>} is the three-digit series number, e.g. C{series001}.
            
        If the delta option is given, then a link is created from the delta directory to each
        new subject session directory, e.g.::
        
            ./delta/subject08/session02 -> ./subject08/session02
        
        @param dirs: the source subject directories
        @return: the target series directories which were added
        """
        # Build the staging area for each subject.
        series = []
        for d in dirs:
            grouped = self._group(d)
            series.extend(grouped)
        return series
    
    def _group(self, path):
        """
        Creates the subject/session staging area in the current working directory.
        Each session subdirectory links the DICOM files in the given source subject
        directories.
        
        See group_dicom_files.
        
        @param path: the source subject directory path
        @return: the target series directories which were added
        """
        src_sbj_dir = os.path.abspath(path)
        # The RE to extract the subject or session number suffix.
        npat = re.compile('\d+$')
        # Extract the subject number from the subject directory name.
        sbj_match = npat.search(os.path.basename(src_sbj_dir))
        if not sbj_match:
            raise StagingError('The source subject directory does not end in a number: ' + src_sbj_dir)
        sbj_nbr = int(sbj_match.group(0))
        # The subject directory which will hold the sessions.
        tgt_sbj_dir_name = "%s%02d" % (self.collection, sbj_nbr)
        tgt_sbj_dir = os.path.abspath(os.path.join(self.dest, tgt_sbj_dir_name))
        if not os.path.exists(tgt_sbj_dir):
            os.makedirs(tgt_sbj_dir)
            logger.debug("Created subject directory %s." % tgt_sbj_dir)
        # Build the target series subdirectories.
        series_dirs = []
        for src_sess_dir in glob.glob(os.path.join(src_sbj_dir, self.session_pat)):
            # Extract the session number from the session directory name.
            sess_match = npat.search(os.path.basename(src_sess_dir))
            if not sess_match:
                raise StagingError('The source session directory does not end in a number: ' + src_sess_dir)
            sess_nbr = int(sess_match.group(0))
            # The session directory which holds the links to the source files.
            tgt_sess_dir_name = "session%02d" % sess_nbr
            tgt_sess_dir = os.path.join(tgt_sbj_dir, tgt_sess_dir_name)
            # Skip an existing session.
            if os.path.exists(tgt_sess_dir):
                if not self.replace:
                    logger.debug("Skipped existing session directory %s." % tgt_sess_dir)
                    continue
            else:
                # Make the target session directory.
                os.mkdir(tgt_sess_dir)
                logger.debug("Created session directory %s." % tgt_sess_dir)
            # Link the delta session directory to the target, if necessary.
            if self.delta_dir:
                delta_sbj_dir = os.path.join(self.delta_dir, tgt_sbj_dir_name)
                if not os.path.exists(delta_sbj_dir):
                    os.makedirs(delta_sbj_dir)
                delta_sess_dir = os.path.join(delta_sbj_dir, tgt_sess_dir_name)
                if os.path.exists(delta_sess_dir):
                    os.remove(delta_sess_dir)
                # The delta link is relative to the target location.
                delta_rel_path = os.path.relpath(tgt_sess_dir, delta_sbj_dir)
                os.symlink(delta_rel_path, delta_sess_dir)
                logger.debug("Linked the delta session directory {0} -> {1}.".format(delta_sess_dir, delta_rel_path))
            # Link each of the DICOM files in the source concatenated subdirectories.
            for src_file in glob.glob(os.path.join(src_sess_dir, self.dicom_pat)):
                if os.path.isdir(src_file):
                    logger.info("Skipped input directory %s." % src_file)
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
                    tgt_series_dir = os.path.join(tgt_sess_dir, "series%03d" % series)
                    if not os.path.exists(tgt_series_dir):
                        os.mkdir(tgt_series_dir)
                        series_dirs.append(tgt_series_dir)
                    # Link the source DICOM file.
                    tgt_file = os.path.join(tgt_series_dir, tgt_file_base)
                    if os.path.islink(tgt_file):
                        if self.replace:
                            os.remove(tgt_file)
                        else:
                            logger.debug("Skipped existing image link %s." % tgt_file)
                            continue
                    # Create a link from the target to the source.
                    os.symlink(src_file, tgt_file)
                    logger.debug("Linked the image file {0} -> {1}".format(tgt_file, src_file))
                else:
                    logger.debug("Skipped non-DICOM file %s." % src_file)
        return series_dirs
    
    def _series_number(self, path):
        """
        :param path: the file path
        :return: the series number, or None if the file is not a DICOM file
        """
        try:
            return read_dicom_header(path).SeriesNumber
        except InvalidDicomError:
            return None
  