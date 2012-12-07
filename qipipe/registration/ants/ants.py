import os
import logging
import envoy
from .ants_error import ANTSError
from .similarity_metrics import *
from .template import create_template
from .warp_transform import WarpTransform

def register(path, output, metric=None, reference=None):
    ANTS(output, metric=metric, reference=reference).register(path)


class ANTS(object):
    """ANTS wraps the ANTS filters.
    """

    def __init__(self, output, metric=None, reference=None):
        """Initializes this ANTS instance.

        :param output: the output directory (default the input directory)
        :param metric: the similarity metric (default cross-correlation)
        :param reference: the fixed reference image (default is generated)
        """
        if not metric:
            metric = CC()
        self.metric = metric
        self.output = os.path.abspath(output)
        self.reference = os.path.abspath(reference)
    
    def register(self, path):
        """Registers the images in the given directory using a symmetric diffeomorphic deformation.

        :param path: the directory containing the images to register
        """
        files = os.listdir(path)
        if not os.path.exists(self.output):
            os.makedirs(self.output)
        # Link to the source images, if necessary.
        if not os.path.samefile(path, self.output):
            self._link_files(path, self.output, files)
        logging.info("Registering the images in %s..." % self.output)
        cwd = os.getcwd()
        os.chdir(self.output)
        try:
            if not self.reference:
                self.reference = create_template(self.metric, files)
            for moving in files:
                WarpTransform(moving, self.reference, self.metric).apply()
        finally:
            os.chdir(cwd)
        logging.info("Registered the images in %s." % self.output)

    def _link_files(self, source, destination, files):
        """
        Links the given files in the source directory to the destination. 
        """
        logging.info("Linking the %(source)s source images to the output %(destination)s..." % {'source': source, 'destination': destination})
        for f in files:
            dest = os.path.join(destination, f)
            if not os.path.exists(dest):
                src = os.path.join(source, f)
                os.symlink(src, dest)
        
    