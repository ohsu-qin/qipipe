"""
This module reorders the OHSU AIRC ``bolero_mask_conv``
result to conform with the time series x and y order.
"""
import os
from nipype.interfaces.base import (
    traits, isdefined, BaseInterfaceInputSpec, BaseInterface, TraitedSpec
)
from qiutil.file import splitexts
from ..helpers import roi


class ReorderBoleroMaskInputSpec(BaseInterfaceInputSpec):
    in_file = traits.Str(desc='Input mask file name', mandatory=True)
    out_file = traits.Str(desc='Output file name')


class ReorderBoleroMaskOutputSpec(TraitedSpec):
    out_file = traits.File(desc='Reordered mask file', exists=True)


class ReorderBoleroMask(BaseInterface):
    """
    Interface to the ROI reordering utility.
    """

    input_spec = ReorderBoleroMaskInputSpec

    output_spec = ReorderBoleroMaskOutputSpec

    def _run_interface(self, runtime):
        in_file = self.inputs.in_file
        # Make the default output file path, if necessary.
        if isdefined(self.inputs.out_file):
            out_file = self.inputs.out_file
        else:
            def_out_file = self._default_output_file_name(in_file)
            out_file = self.inputs.out_file = def_out_file

        roi.reorder_bolero_mask(in_file, out_file=out_file)

        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        # Capture the expanded output path.
        outputs['out_file'] = os.path.abspath(self.inputs.out_file)

        return outputs

    def _default_output_file_name(self, in_file):
        """
        The default output file name appends ``_reordered``
        to the input file base name.
        """
        _, in_file_name = os.path.split(in_file)
        base, ext = splitexts(in_file_name)
        return base + '_reordered' + ext
