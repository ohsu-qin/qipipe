import os
import logging
import envoy
from .similarity_metrics import *
from .ants_error import ANTSError

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
        logging.info("Registration template already exists: %s" % tmpl)
        return tmpl
    
    # Build the command line.
    if not metric:
        metric = PR()
    opts = "-d %(dim)s -j %(cores)s -m %(metric)s" % {'dim': dimension, 'cores': cores, 'metric': metric}
    cmd = CMD.format(output=PREFIX, opts=opts, files=' '.join(files))
    logging.info("Building the %s registration template with the following command:" % tmpl)
    logging.info(cmd)
    
    # Run the command.
    r = envoy.run(cmd)
    if r.status_code:
        logging.error("Build registration template failed with error code %d" % r.status_code)
        logging.error(r.std_err)
        raise ANTSError("Build registration template unsuccessful; see the log for details")
    if not os.path.exists(tmpl):
        logging.error("Build registration template was not created.")
        raise ANTSError("Build registration template unsuccessful; see the log for details")
        
    logging.info("Built the registration template %s." % tmpl)
    return tmpl
