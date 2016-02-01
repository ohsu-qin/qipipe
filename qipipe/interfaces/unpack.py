from nipype.interfaces.base import (traits, DynamicTraitedSpec, isdefined)
from nipype.interfaces.io import (IOBase, add_traits)
from .interface_error import InterfaceError


class Unpack(IOBase):
    """
    The Unpack Interface converts a list input field to one output field per list item.
    
    Example:
    
    >>> from qipipe.interfaces.unpack import Unpack
    >>> unpack = Unpack(input_name='list', output_names=['a', 'b'], list=[1, 2])
    >>> result = unpack.run()
    >>> result.outputs.a
    1
    >>> result.outputs.b
    2
    """
    
    input_spec = DynamicTraitedSpec
    
    output_spec = DynamicTraitedSpec
    
    def __init__(self, input_name, output_names, mandatory_inputs=True, **inputs):
        """
        :param input_name: the input list field name
        :param output_names: the output field names
        :param mandatory_inputs: a flag indicating whether every input field is
            required
        :param inputs: the input field name => value bindings
        """
        super(Unpack, self).__init__(**inputs)
        if not input_name:
            raise InterfaceError('Unpack input field name is missing')
        if not output_names:
            raise InterfaceError('Unpack output field names must be a non-empty list')
        self.input_names = [input_name]
        self.output_names = output_names
        self._mandatory_inputs = mandatory_inputs
        add_traits(self.inputs, self.input_names, traits.List)
        # Adding any traits wipes out all input values set in superclass
        # initialization, even it the trait is not in the add_traits argument.
        # The work-around is to reset the values after adding the traits.
        self.inputs.set(**inputs)
    
    def _add_output_traits(self, base):
        return add_traits(base, self.output_names)
    
    def _run_interface(self, runtime):
        # Manual mandatory inputs check.
        if self._mandatory_inputs:
            for key in self.input_names:
                value = getattr(self.inputs, key)
                if not isdefined(value):
                    msg = ("%s requires a value for input '%s' because it was"
                        " listed in 'input_names'. You can turn off mandatory"
                        " inputs checking  by passing mandatory_inputs = False"
                        " to the constructor." % (self.__class__.__name__, key))
                    raise InterfaceError(msg)
        
        # The input list.
        in_list = getattr(self.inputs, self.input_names[0])
        # The output name => value dictionary result.
        self._result = {self.output_names[i]: value for i, value in enumerate(in_list)}
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        for key, value in self._result.iteritems():
            outputs[key] = value
        return outputs
