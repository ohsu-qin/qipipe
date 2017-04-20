"""
This module reorders the OHSU AIRC ``bolero_mask_conv``
result to conform with the time series x and y order.
"""
import os
from os import path
from glob import glob
import traits.api as traits
from nipype.interfaces.base import (TraitedSpec, CommandLine,
                                    CommandLineInputSpec)
from nipype.interfaces.traits_extension import Undefined
from qiutil.file import splitexts


class ReorderBoleroMaskInputSpec(CommandLineInputSpec):
    in_file = traits.Str(desc='mask file', mandatory=True,
                         position=1, argstr='\"%s\"')
    out_file = traits.Str(desc='Output file name', argstr='-o %s')


class ReorderBoleroMaskOutputSpec(TraitedSpec):
    out_file = traits.File(desc='Reordered mask file', exists=True)


class ReorderBoleroMask(CommandLine):
    """
    Interface to the mask reordering utility.
    """

    _cmd = 'bolero_mask_reorder'
    input_spec = ReorderBoleroMaskInputSpec
    output_spec = ReorderBoleroMaskOutputSpec

    def __init__(self, **inputs):
        super(ReorderBoleroMask, self).__init__(**inputs)

    def run(self, **inputs):
        if not inputs.get('out_file'):
            in_file = inputs.get('in_file')
            # in_file is mandatory, but let that be checked by the
            # base class and the appropriate error thrown there.
            if in_file:
                inputs['out_file'] = self._default_output_file_name(in_file)
        super(ReorderBoleroMask, self).run(**inputs)

    def _list_outputs(self):
        outputs = self._outputs().get()
        out_file = self.inputs.out_file
        # Expand the output path, if necessary.
        outputs['out_file'] = os.path.abspath(out_file)

        return outputs

    def _default_output_file_name(self, in_file):
        """
        The default output file name appends ``_reordered``
        to the input file base name.
        """
        _, in_file_name = os.path.split(in_file)
        base, ext = splitexts(in_file_name)
        return base + '_reordered' + ext
