import os
import shutil
import logging
from .similarity_metrics import *
from .template import create_template
from .warp_transform import warp


"""
Registers the images in the given path.

If the reference fixed image is missing, then it is generated by calling
create_dce_template.

:param path: the directory containing the images to register
:param output: the output directory (default is the input directory)
:param work: the working directory for intermediate results (default is the output directory)
:param metric: the similarity metric (default is cross-correlation)
:param reference: the fixed reference image (default is generated)
:return: the source => registered file dictionary
"""
def register(path, output=None, work=None, metric=None, reference=None):
    return ANTS(output=output, work=work, metric=metric, reference=reference).register(path)


def create_dce_template(files, metric=None):
    """
    Generates a fixed reference template for registering the given image files.
    
    If there more than 10 but less than 100 input files, then the reference
    image is generated from at most 50 input files starting at the fifth input file
    in sort order.
    
    If there are at least 100 input files, then the reference image is generated from
    50 input files consisting of a uniformally-sampled subset starting at the tenth
    input file and separated by no more than five input files in sort order.
    For example, if there are 225 images named image0001.dcm to image0225.dcm, then
    the reference template averages the files image0010.dcm, image0014.dcm, ...,
    image0206.dcm, image0210.dcm.
    
    Otherwise, if there are 10 or fewer input files, then the reference image
    is generated from all of the given input files.

    :param files: the images to average
    :param metric: the similarity metric (default is cross-correlation)
    :return: the reference template
    """
    # If there are many input files, then build the template with a subset.
    if len(files) >= 100:
        tfiles = []
        step = min(5, (len(files) - 20) / 50)
        for i in range(0, 49):
            tfiles.append(files[9 + i * step])
    elif len(files) > 10:
        tfiles = files[4:min(54, len(files))]
    else:
        tfiles = files
    return create_template(tfiles, metric=metric)
    
    
class ANTS(object):
    """ANTS wraps the ANTS filters.
    """

    def __init__(self, output=None, work=None, metric=None, reference=None):
        """Initializes this ANTS instance.

        :param output: the output directory (default is the input directory)
        :param work: the working directory for intermediate results (default is the output directory)
        :param metric: the similarity metric
        :param reference: the fixed reference image (default is generated)
        """
        self.metric = metric
        self.output = output
        self.work = work
        # Make the reference an absolute path, since register switches to the work directory. 
        if reference:
            self.reference = os.path.abspath(reference)
        else:
            self.reference = None
    
    def register(self, path):
        """Registers the images in the given directory using a symmetric diffeomorphic deformation.

        :param path: the directory containing the images to register
        :return: the source => registered file name dictionary
        """
        files = os.listdir(path)
        files.sort()
        if self.work:
            work = self.work
        elif self.output:
            work = self.output
        else:
            work = path
        if not os.path.exists(work):
            os.makedirs(work)
        if not os.path.samefile(path, work):
            # Link to the source images, if necessary.
            self._link_files(path, work, files)
        registered = dict()
        logging.info("Registering the images in %s..." % work)
        cwd = os.getcwd()
        os.chdir(work)
        try:
            if not self.reference:
                self.reference = create_dce_template(files, self.metric)
            for moving in files:
                registered[moving] = warp(moving, self.reference, self.metric)
        finally:
            os.chdir(cwd)
        logging.info("Registered the images in %s." % work)
        if self.output:
            if not os.path.exists(self.output):
                os.makedirs(self.output)
            if not os.path.samefile(self.output, work):
                logging.info("Copying the registered images from %(source)s to %(destination)s..." % {'source': work, 'destination': self.output})
                for fn in registered.values():
                    wf = os.path.join(work, fn)
                    of = os.path.join(self.output, fn)
                    if os.path.exists(of):
                        os.remove(of)
                    shutil.copyfile(wf, of)
                logging.info("Copied the registered images to %s." % self.output)
        return registered

    def _link_files(self, source, destination, files):
        """
        Links the given files in the source directory to the destination. 
        """
        logging.info("Linking the %(source)s images to the destination %(destination)s..." % {'source': source, 'destination': destination})
        source = os.path.abspath(source)
        for f in files:
            dest = os.path.join(destination, f)
            if os.path.exists(dest):
                os.remove(dest)
            src = os.path.join(source, f)
            os.symlink(src, dest)
        
    