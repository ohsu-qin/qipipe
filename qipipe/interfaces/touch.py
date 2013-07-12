import os
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    File)


class TouchInputSpec(BaseInterfaceInputSpec):
    fname = File(mandatory=True, desc='The file to create')


class TouchOutputSpec(TraitedSpec):
    fname = File(exists=True, desc='The created file')


class Touch(BaseInterface):
    """
    The Touch interface emulates the Unix ``touch`` command.
    This interface is useful for stubbing out processing
    nodes during workflow development.
    """
    input_spec = TouchInputSpec
    
    output_spec = TouchOutputSpec

    def _run_interface(self, runtime):
        if os.path.exists(self.inputs.fname):
            os.utime(self.inputs.fname, None)
        else:
            parent, _ = os.path.split(self.inputs.fname)
            if parent and not os.path.exists(parent):
                os.makedirs(parent)
            open(self.inputs.fname, 'w').close()

        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['fname'] = self.inputs.fname
        
        return outputs
