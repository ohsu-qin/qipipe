import os
from matplotlib import (pyplot, colors, cm)
import numpy as np
import nibabel as nib
from qiutil.logging import logger


def normalize(value, vmin, vspan):
    """
    Maps the given input value to [0, 1].
    
    :param value: the input value
    :param vmin: the minimum input range value
    :param vspan: the value range span (maxium - minimum)
    :return: (*in_val* - *vmin*) / *vspan*
    """
    return (value - vmin) / vspan


def discretize(in_file, out_file, nvalues, start=0, threshold=None,
               normalizer=normalize):
    """
    Transforms the given input image file to an integer range with
    the given number of values. The range starts at the given start
    value. The input values are uniformly mapped into the output
    range. For example, if the input values range from 0.0 to 3.0
    *nvalues* is 101, and the start is 0, then an input value of 0
    is transformed to 0, 3.0 is transformed to 100, and the intermediate
    input values are proportionately transformed to the output range.
    
    If a threshold is specified, then every input value which maps to
    an output value less than (*threshold* * *nvalues*) - *start* is
    transformed to the output start value. For example, if the input
    values range from 0.0 to 3.0, then::
        
        discretize(in_file, out_file, 1001, threshold=0.5)
    
    transforms input values as follows:
    
    * If the input value maps to the first half of the output range,
      then the output value is 0.
    
    * Otherwise, the input value *v* maps to the output value
      (*v* * 1000) / 3.
    
    :param in_file: the input file path
    :param out_file: the output file path
    :param nvalues: the number of output entries
    :param start: the starting output value (default 0)
    :param threshold: the threshold in the range start to nvalues
      (default start)
    :param normalize: an optional function to normalize the input
      value (default :meth:`normalize`)
    :raise IndexError: if the threshold is not in the color range
    """
    # If there is an output file argument, then ensure that there is an
    # output parent directory. Otherwise, use the default output file.
    
    # The logger.
    log = logger(__name__)
    
    # The default threshold is 0.
    if not threshold:
        threshold = start
    # Validate the threshold.
    if threshold < start or threshold >= start + nvalues:
        raise IndexError("The threshold is not in the color range"
                         " [%d-%d]: %d" %
                         (start, start + nvalues, threshold))
    
    log.debug("Color LUT start: %d end: %d threshold: %d" %
              (start, start + nvalues - 1, threshold))
    
    # Load the NIfTI image.
    in_img = nib.load(in_file)
    log.debug("Loaded %s." % in_file)
    # The image data 3D array.
    in_data = in_img.get_data()
    
    # Compute the minimum and maximum value.
    vmin = float('infinity')
    vmax = float('-infinity')
    # Each data x value is a [y][z] 2D array.
    for yz in in_data:
        # Iterate over the z arrays.
        for z in yz:
            vmin = min(vmin, *z)
            vmax = max(vmax, *z)
    
    log.debug("Computed value minimum %f." % vmin)
    log.debug("Computed value maximum %f." % vmax)
    
    # The maximum offset is one less than the number of values.
    max_offset = nvalues - 1
    # The length of the value range.
    vspan = vmax - vmin
    # Track the value count by decile.
    decile_cnts = [0] * 10
    
    # The target image data is an array of shorts with the
    # same shape as the input data.
    shape = in_data.shape
    out_data = np.empty(shape, dtype=np.int16)
    
    # Normalize the values into the discrete range.
    for i in range(shape[0]):
        for j in range(shape[1]):
            for k in range(shape[2]):
                # The input voxel value.
                in_val = in_data[i][j][k]
                # The proportional offset into the range.
                proportion = normalizer(in_val, vmin, vspan)
                # The offset in the [0..nvalues - 1] range.
                offset = int(round(proportion * max_offset))
                # The output value.
                out_val = start + offset
                # Check against the threshold.
                if out_val < threshold:
                    offset = 0
                    out_val = start
                # Set the output voxel value.
                out_data[i][j][k] = out_val
                # Increment the output value decile count.
                di = (offset * 10) / nvalues
                decile_cnts[di] += 1
    
    # Log the total number of values and the decile count.
    value_cnt = reduce(lambda x,y: x * y, shape)
    log.debug("%d input values were mapped to the inclusive range [%d, %d]." %
              (value_cnt, start, start + nvalues - 1))
    log.debug("Mapped value decile count: %s." % decile_cnts)
    
    log.debug("Saving the output as %s..." % out_file)
    hdr = in_img.get_header()
    hdr.set_data_dtype(np.int16)
    out_img = nib.Nifti1Image(out_data, in_img.get_affine(), hdr)
    out_img.to_filename(out_file)
