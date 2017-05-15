"""
This module reorders the OHSU AIRC ``bolero_mask_conv``
result to conform with the time series x and y order.
"""
import os
import traits.api as traits
from nipype.interfaces.base import (TraitedSpec, CommandLine,
                                    CommandLineInputSpec)
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

    _cmd = 'reorder_bolero_mask'
    input_spec = ReorderBoleroMaskInputSpec
    output_spec = ReorderBoleroMaskOutputSpec

    def __init__(self, **inputs):
        super(ReorderBoleroMask, self).__init__(**inputs)

    def run(self, **inputs):
        self._out_file = inputs.get('out_file')
        if not self._out_file:
            in_file = inputs.get('in_file')
            # in_file is mandatory, but superclass run method check
            # that and throw the appropriate error.
            if in_file:
                self._out_file = self._default_output_file_name(in_file)
        super(ReorderBoleroMask, self).run(**inputs)

    def _list_outputs(self):
        outputs = self._outputs().get()
        # Expand the output path, if necessary.
        outputs['out_file'] = os.path.abspath(self._out_file)

        return outputs

    def _default_output_file_name(self, in_file):
        """
        The default output file name appends ``_reordered``
        to the input file base name.
        """
        _, in_file_name = os.path.split(in_file)
        base, ext = splitexts(in_file_name)
        return base + '_reordered' + ext
