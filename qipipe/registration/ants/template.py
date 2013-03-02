import os
import envoy
from .similarity_metrics import *
from .ants_error import ANTSError
from .environment import ants_environ

import logging
logger = logging.getLogger(__name__)

def create_template(files, dimension=2, cores=4, metric=None):
    """
    Builds a template from the given image files.
    
    @param files: the image files
    @param dimension: the image dimension - 2 (default), 3, or 4
    @param cores: the number of CPU cores (default 4)
    @param metric: the similarity metric (default probability mapping)
    @return: the template file name
    """
    CMD = "buildtemplateparallel.sh -c 2 {opts} -o {output} {files}"
    PREFIX = 'reg_'
    SUFFIX = 'template.nii.gz'

    # The template file name.
    tmpl = PREFIX + SUFFIX
    if os.path.exists(tmpl):
        logger.info("Registration template already exists: %s" % tmpl)
        return tmpl
    
    # Build the command line.
    if not metric:
        metric = PR()
    opts = "-d %(dim)s -j %(cores)s -m %(metric)s" % {'dim': dimension, 'cores': cores, 'metric': metric}
    cmd = CMD.format(output=PREFIX, opts=opts, files=' '.join(files))
    logger.info("Building the %s registration template with the following command:" % tmpl)
    logger.info(cmd)
    
    # Run the command.
    ants_path = os.getenv('ANTSPATH')
    if not ants_path:
        raise ANTSError("ANTSPATH environment variable is not set.")
    r = envoy.run(cmd, env=ants_environ())
    if r.status_code:
        logger.error("Build registration template failed with error code %d" % r.status_code)
        logger.error(r.std_err)
        raise ANTSError("Build registration template unsuccessful; see the log for details")
    if not os.path.exists(tmpl):
        logger.error("Build registration template was not created: %s." % tmpl)
        raise ANTSError("Build registration template unsuccessful; see the log for details")
        
    logger.info("Built the registration template %s." % tmpl)
    return tmpl
