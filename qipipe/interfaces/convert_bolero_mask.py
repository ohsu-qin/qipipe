"""
This module wraps the proprietary OHSU AIRC ``bolero_mask_conv``
utility. ``bolero_mask_conv`` converts a proprietary OHSU format
mask file into a NiFTI mask file.
"""
import os
from os import path
from glob import glob
import traits.api as traits
from nipype.interfaces.base import (TraitedSpec, CommandLine,
                                    CommandLineInputSpec)
from nipype.interfaces.traits_extension import Undefined


class ConvertBoleroMaskInputSpec(CommandLineInputSpec):
    time_series = traits.File(desc='Input 4D DCE series NiFTI file',
                              mandatory=True, exists=True, position=1,
                              argstr='%s')
    slice_index = traits.Int(desc='One-based slice index', mandatory=True,
                             position=2, argstr='%d')
    in_file = traits.Int(desc='BOLERO .bqf mask file', mandatory=True,
                         position=3, argstr='%d')
    out_base = traits.Str(desc='Output file base name without extension',
                          argstr='-o %s')


class ConvertBoleroMaskOutputSpec(TraitedSpec):
    out_file = traits.File(desc='NiFTI mask file', exists=True)


class ConvertBoleroMask(CommandLine):
    """
    Interface to the proprietary OHSU AIRC ``bolero_mask_conv`` utility.
    """

    _cmd = 'bolero_mask_conv'
    input_spec = ConvertBoleroMaskInputSpec
    output_spec = ConvertBoleroMaskOutputSpec

    def __init__(self, **inputs):
        super(ConvertBoleroMask, self).__init__(**inputs)

    def _list_outputs(self):
        outputs = self._outputs().get()

        # The default output base name is slice_<slice>_lesion.
        out_base = self.inputs.out_base or "slice_%d_lesion" % self.inputs.slice_index
        # The output is compressed.
        out_file = out_base + '.nii.gz'
        # Expand the output path.
        outputs['out_file'] = os.path.abspath(out_file)

        return outputs
