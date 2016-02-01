import os
import re
from matplotlib import (pyplot, colors, cm)
import nibabel as nib
from qiutil.file import splitexts
from . import image


def create_lookup_table(ncolors, colormap='jet', out_file=None):
    """
    Generates a colormap lookup table with the given number of colors.
    
    :param ncolors: the number of colors to generate
    :param colormap: the matplotlib colormap name
    :param out_file: the output file path (default is the colormap
      name followed by ``_colors.txt``in the current directory)
    """
    # If there is an output file argument, then ensure that there is an
    # output parent directory. Otherwise, use the default output file.
    if out_file:
        out_dir = os.path.dirname(out_file)
        os.makedirs(out_dir)
    else:
        out_file = colormap + '_colors.txt'
    
    # The LUT reference values.
    values = range(ncolors)
    # The color map.
    cmap = pyplot.get_cmap(colormap)
    # The color range normalizer.
    norm = colors.Normalize(vmin=0, vmax=values[-1])
    # The value -> color mapper.
    scalar_map = cm.ScalarMappable(norm=norm, cmap=cmap)
    # The RGBA analog of the values.
    rgbas = scalar_map.to_rgba(values, bytes=True)
    
    # Save the LUT.
    print ("Saving the colormap as %s..." % out_file)
    with open(out_file, 'w') as f:
        # Write the initial background color.
        # Note - the LUT field separator must be one blank;
        # white space, e.g. a tab, is not supported by some
        # renderers, e.g. XTK.
        line = '0 0 0 0 0 0\n'
        f.write(line)
        # Write the colormap colors.
        for i, rgba in enumerate(rgbas):
            r, g, b, a = rgba
            line = "%d %d %d %d %d %d\n" % (i + 1, i + 1, r, g, b, a)
            f.write(line)


def colorize(lut_file, *inputs, **opts):
    """
    Transforms each input voxel value to a color lookup table
    reference.
    
    The input voxel values are uniformly partitioned for the given
    colormap LUT. For example, if the voxel values range from 0.0 to
    3.0, then the a voxel value of 0 transformed to the first LUT color,
    3.0 is transformed to the last LUT color, and the intermediate
    values are transformed to intermediate colors.
    
    The voxel -> reference output file name appends ``_color`` to the
    input file basename and preserves the input file extensions, e.g.
    the input file ``k_trans_map.nii.gz`` is transformed to
    ``k_trans_map_color.nii.gz`` in the output directory.
    
    :param lut_file: the color lookup table file location
    :param inputs: the image files to transform
    :param opts: the following keyword arguments:
    :option dest: the destination directory
        (default current working directory)
    :option threshold: the threshold in the range 0 to nvalues
        (default 0)
    """
    dest = opts.pop('dest', None)
    if dest:
        if not os.path.exists(dest):
            os.makedirs(dest)
    else:
        dest = os.getcwd()
    opts.update(_infer_range_parameters(lut_file))
    opts['normalizer'] = _normalize
    for in_file in inputs:
        _colorize(in_file, dest, **opts)


def label_map_basename(location):
    """
    :param location: the input file path
    :return: the corresponding color file name
    """
    # Split up the input file path.
    _, basename = os.path.split(location)
    base, exts = splitexts(basename)
    
    return base + '_color' + exts


def _normalize(value, vmin, vspan):
    # Zero always maps to the translucence in the first colormap LUT entry.
    if value == 0:
        return 0
    else:
        return image.normalize(value, vmin, vspan)


def _colorize(in_file, dest, **opts):
    # The output file base name.
    base = label_map_basename(in_file)
    # The output file location.
    out_file = os.path.join(dest, base)
    # Make the output file.
    image.discretize(in_file, out_file, **opts)


def _infer_range_parameters(lut_file):
    with open(lut_file) as f:
        content = f.readlines()
        nvalues = len(content)
        start = min(_labels(content))
        
        return dict(nvalues=nvalues, start=start)


LABEL_REGEX = re.compile('\d+')


def _labels(content):
    """Label generator for the given colormap LUT content."""
    for line in content:
        match = LABEL_REGEX.match(line)
        # Could be a comment, so ignore if no match.
        if match:
            yield int(match.group(0))

