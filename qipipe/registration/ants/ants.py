import os
import shutil
import logging
from .similarity_metrics import *
from .template import create_template
from .warp_transform import warp


"""Registers the images in the given path.

:param path: the directory containing the images to register
:param output: the output directory (default is the input directory)
:param work: the working directory for intermediate results (default is the output directory)
:param metric: the similarity metric (default is cross-correlation)
:param reference: the fixed reference image (default is generated)
:return: the source => registered file dictionary
"""
def register(path, output=None, work=None, metric=None, reference=None):
    return ANTS(output=output, work=work, metric=metric, reference=reference).register(path)


class ANTS(object):
    """ANTS wraps the ANTS filters.
    """

    def __init__(self, output=None, work=None, metric=None, reference=None):
        """Initializes this ANTS instance.

        :param output: the output directory (default is the input directory)
        :param work: the working directory for intermediate results (default is the output directory)
        :param metric: the similarity metric (default is cross-correlation)
        :param reference: the fixed reference image (default is generated)
        """
        if not metric:
            metric = CC()
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
                self.reference = create_template(self.metric, files)
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
        
    