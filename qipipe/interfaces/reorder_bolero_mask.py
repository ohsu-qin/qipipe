"""
This module reorders the OHSU AIRC ``bolero_mask_conv``
result to conform with the time series x and y order.
"""
import os
from nipype.interfaces.base import (traits, BaseInterface, TraitedSpec)
from qiutil.file import splitexts


class ReorderBoleroMaskInputSpec(CommandLineInputSpec):
    in_file = traits.Str(desc='Input mask file name', mandatory=True,
                         position=1, argstr='\"%s\"')
    out_file = traits.Str(desc='Output file name', argstr='-o %s')


class ReorderBoleroMaskOutputSpec(TraitedSpec):
    out_file = traits.File(desc='Reordered mask file', exists=True)


class ReorderBoleroMask(BaseInterface):
    """
    Interface to the mask reordering utility.
    """

    _cmd = 'reorder_bolero_mask'
    input_spec = ReorderBoleroMaskInputSpec
    output_spec = ReorderBoleroMaskOutputSpec

    def __init__(self, **inputs):
        super(ReorderBoleroMask, self).__init__(**inputs)


    def _run_interface(self, runtime):
        self._out_file = self._copy(self.inputs.in_file, self.inputs.dest,
                                    self.inputs.out_fname)

    def _run_interface(self, **inputs):
        if not inputs.get('out_file'):
            in_file = inputs.get('in_file')
            # in_file is mandatory, but let the superclass run
            # method check that and throw the appropriate error.
            if in_file:
                def_out_file = self._default_output_file_name(in_file)
                inputs['out_file'] = self.inputs.out_file = def_out_file


        print (">>rbm inputs: %s" % inputs)
        print (">>rbm self.inputs.out_file: %s" % self.inputs.out_file)

        return runtime


        super(ReorderBoleroMask, self).run(**inputs)

    def _list_outputs(self):
        outputs = self._outputs().get()
        # Expand the output path, if necessary.
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
