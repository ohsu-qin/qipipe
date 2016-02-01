import os
from nipype.interfaces.base import (BaseInterfaceInputSpec, TraitedSpec,
                                    BaseInterface, File)


class TouchInputSpec(BaseInterfaceInputSpec):
    in_file = File(mandatory=True, desc='The file to touch')


class TouchOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The touched file')


class Touch(BaseInterface):
    """
    The Touch interface emulates the Unix ``touch`` command.
    This interface is useful for stubbing out processing
    nodes during workflow development.
    """
    input_spec = TouchInputSpec
    
    output_spec = TouchOutputSpec
    
    def _run_interface(self, runtime):
        self._file = os.path.abspath(self.inputs.in_file)
        parent, _ = os.path.split(self._file)
        if parent:
            if not os.path.exists(parent):
                os.makedirs(parent)
            else:
                parent = os.getcwd()
        if os.path.exists(self._file):
            os.utime(self._file, None)
        else:
            open(self._file, 'w').close()
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._file
        
        return outputs
