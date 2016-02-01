from nipype.interfaces.base import (
    traits, BaseInterfaceInputSpec, TraitedSpec)
from nipype.interfaces.io import IOBase


class LookupInputSpec(BaseInterfaceInputSpec):
    key = traits.Any(mandatory=True, desc='The dictionary key')
    
    dictionary = traits.Dict(mandatory=True, desc='The dictionary')


class LookupOutputSpec(TraitedSpec):
    value = traits.Any(desc='The lookup value')


class Lookup(IOBase):
    """
    The Lookup Interface wraps a dictionary look-up.
    
    Example:
    
    >>> from qipipe.interfaces import Lookup
    >>> lookup = Lookup(key='a', dictionary=dict(a=1, b=2))
    >>> result = lookup.run()
    >>> result.outputs.value
    1
    """
    
    input_spec = LookupInputSpec
    
    output_spec = LookupOutputSpec
    
    def _run_interface(self, runtime):
        # The output name => value dictionary result.
        self._result = self.inputs.dictionary[self.inputs.key]
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['value'] = self._result
        
        return outputs
