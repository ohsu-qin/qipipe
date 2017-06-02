"""
OHSU - This module wraps the proprietary OHSU AIRC ``dce_to_r1``
utility.
"""

# TODO - move this to the ohsu-qin.pipeline.interfaces module

# Absolute import (the default in a future Python release) resolves
# the dce_to_r1 import as the installed dce_to_r1 Python module rather
# than this interface of the same name.
from __future__ import absolute_import
import os
from nipype.interfaces.base import (
    traits, TraitedSpec, CommandLine, CommandLineInputSpec
)

OUTPUT_FILE_NAME = 'r1_series.nii.gz'
"""The dce_to_r1 output R1 file base name."""

MASK_FILE_NAME = 'valid_mask.nii.gz'
"""The dce_to_r1 output mask file base name."""

class DceToR1InputSpec(CommandLineInputSpec):
    in_file = traits.File(desc='The 4D time series image file path',
                          mandatory=True, position=1, argstr='%s')

    r1_0_val = traits.Float(desc='Constant R1 value', mandatory=True,
                            position=2, argstr='%d')

    # TODO - add r1_0_file as an alternative input.
    #r1_0_file = traits.File(desc='R1 2D or 3D image file path',
    #                        mandatory=???, position=???, argstr='%s')

    base_end = traits.String(desc='End index for baseline signal',
                             argstr='--base-end %s')

    mask = traits.File(desc='Mask file', argstr='--mask %s')

    out_dir = traits.File(desc='Target directory to place the results',
                          argstr='--out_dir %s')


class DceToR1OutputSpec(TraitedSpec):
    out_file = traits.File(desc='Output R1 series file path', exists=True)

    mask = traits.File(desc='Mask file with invalid R1 voxels set to 0')


class DceToR1(CommandLine):
    """
    Convert a T1-weighted DCE time series of signal intensities to a
    series of R1 values.
    """

    _cmd = 'dce_to_r1'

    input_spec = DceToR1InputSpec

    output_spec = DceToR1OutputSpec

    def __init__(self, **inputs):
        super(DceToR1, self).__init__(**inputs)

    def _list_outputs(self):
        # Find the output file in the output directory.
        out_dir_opt = self.inputs.out_dir
        out_dir = out_dir_opt if out_dir_opt else os.getcwd()
        out_file = "%s/%s" % (out_dir, OUTPUT_FILE_NAME)
        mask = "%s/%s" % (out_dir, MASK_FILE_NAME)
        # Set the output.
        outputs = self._outputs().get()
        outputs['out_file'] = out_file
        outputs['mask'] = mask

        return outputs
