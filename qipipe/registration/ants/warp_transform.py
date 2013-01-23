import os
from qipipe.helpers.logging import logger
import envoy
from .similarity_metrics import *
from .ants_error import ANTSError

def warp(moving, fixed, metric=None):
    """
    Warps the given moving image to fit the fixed image. The result is a new image
    named by the moving image name without extension followed by 'Registered.nii.gz',
    e.g. 'image0004Registered.nii.gz'.
    
    @param moving: the file name of the image to transform
    @param fixed: the file name of the reference image
    :parm metric: the similarity metric (default cross-correlation)
    @return: the name of the new image file
    """
    return WarpTransform(moving, fixed, metric).apply()

class WarpTransform:
    """An ANTS WarpTransform applies a deformation field and affine transform to an image."""
    
    def __init__(self, moving, fixed, metric=None, iterations=[100,100,10]):
        """
        @param moving: the file name of the image to transform
        @param fixed: the file name of the reference image
        @param metric: the similarity metric (default cross-correlation)
        @param iterations: the number of iterations in each resolution
        @return: the new transform
        @rtype: WarpTransform
        """
        MAP = "ANTS 2 -m {metric} -i {iterations} -r Gauss[3,0] -t SyN[0.25] -o {output}"
        FIELD = "{output}Warp.nii.gz"
        AFFINE = "{output}Affine.txt"

        self.moving = moving
        self.fixed = fixed
        if not metric:
            metric = CC()
        mstr = metric.format(fixed=fixed, moving=moving)
        name = os.path.splitext(moving)[0]
        cmd = MAP.format(metric=mstr, output=name, iterations='x'.join(iterations))
        logger.info("Building the %(moving)s -> %(fixed)s warp transform with the following command:" % {'moving': moving, 'fixed': fixed})
        logger.info(cmd)
        r = envoy.run(cmd)
        if r.status_code:
            logger.error("Build transform failed with error code %d" % r.status_code)
            logger.error(r.std_err)
            raise ANTSError("Build transform unsuccessful; see the log for details")
        self.field = FIELD.format(output=name)
        self.affine = AFFINE.format(output=name)
        logger.info("The %(moving)s -> %(fixed)s deformation field is %(field)s" % {'moving': moving, 'fixed': fixed, 'field': self.field})
        logger.info("The %(moving)s -> %(fixed)s affine transform is %(affine)s" % {'moving': moving, 'fixed': fixed, 'affine': self.affine})
    
    def apply(self):
        """
        Applies this transform to the given moving image. The result is a new image named by
        the moving image name without extension followed by 'Registered.nii.gz', e.g.
        'image0004Registered.nii.gz'.
        
        @return: the name of the new image file
        """
        TARGET = "{output}Registered.nii.gz"
        REG = "WarpImageMultiTransform 2 {moving} {target} -R {fixed} {field} {affine}"
        
        target = TARGET.format(output=os.path.splitext(self.moving)[0])
        cmd = REG.format(fixed=self.fixed, moving=self.moving, target=target, field=self.field, affine=self.affine)
        logger.info("Applying the %(moving)s -> %(fixed)s warp transform with the following command:" % {'moving': self.moving, 'fixed': self.fixed})
        logger.info(cmd)
        r = envoy.run(cmd)
        if r.status_code:
            logger.error("Apply warp transform failed with error code %d" % r.status_code)
            logger.error(r.std_err)
            raise ANTSError("Apply warp transform unsuccessful; see the log for details")
        logger.info("Applied the %(moving)s -> %(fixed)s warp transform with result %(target)s" % {'moving': self.moving, 'fixed': self.fixed, 'target': target})
        return target