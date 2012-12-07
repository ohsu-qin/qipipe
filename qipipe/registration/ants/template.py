import os
import logging
import envoy
from .ants_error import ANTSError

def create_template(metric, files):
    """
    Builds a template from the given image files.
    
    :param metric: the similarity metric
    :param files: the image files
    :return: the template file name
    """
    CMD = "buildtemplateparallel.sh -d 2 -c 2 -j 4 -d 2 -s {metric} -o {output} {files}"
    PREFIX = 'reg_'
    SUFFIX = 'template.nii.gz'

    tmpl = PREFIX + SUFFIX
    if os.path.exists(tmpl):
        logging.info("Registration template already exists: %s" % tmpl)
        return tmpl
    cmd = CMD.format(metric=metric.name, output=PREFIX, files=' '.join(files))
    logging.info("Building the %s registration template with the following command:" % tmpl)
    logging.info(cmd)
    r = envoy.run(cmd)
    if r.status_code:
        logging.error("Build registration template failed with error code %d" % r.status_code)
        logging.error(r.std_err)
        raise ANTSError("Build registration template unsuccessful; see the log for details")
    logging.info("Built the registration template %s." % tmpl)
    return tmpl
