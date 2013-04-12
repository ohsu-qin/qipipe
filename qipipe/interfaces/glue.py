from nipype.interfaces.base import (traits, Undefined, DynamicTraitedSpec, isdefined)
from nipype.interfaces.io import IOBase, add_traits
from ..helpers.collection_helper import is_nonstring_iterable

class GlueError(Exception):
    pass
    
class Glue(IOBase):
    """
    The Glue Interface converts input fields to output fields as follows:
        - If a transformer function is defined, then that function is called with arguments
          set to the corresponding input fields
        - Otherwise, if there are the same number of input field names as output field names,
          then each output field is set to the input field with the same index.
        - Otherwise, if there is one non-string iterable input field, then the output fields
          are set to the the input field list items.
        - Otherwise, if there is one output field, then the output is set to a list consisisting
          of the input field values.
        - Otherwise, a L{GlueError} is raised.
    
    Examples:
        >>> glue = Glue(input_names=['foo'], output_names=['bar'], foo='foobar')
        >>> glue.run().outputs.bar
        'foobar'
        
        >>> glue = Glue(input_names=['foo'], output_names=['bar', 'baz'], foo=['foobar', 'foobaz'])
        >>> result = glue.run()
        >>> result.outputs.bar
        'foobar'
        >>> result.outputs.baz
        'foobaz'
        
        >>> glue = Glue(input_names=['foo', 'bar'], output_names=['baz'], foo='foo', bar='bar')
        >>> result = glue.run()
        >>> result.outputs.baz
        ['foo', 'bar']
        
        >>> def parse_name(name):
        ...     words = name.split()
        ...     result = dict(first=words.pop(0), last=words.pop())
        ...     if words:
        ...         result['middle'] = ' '.join(words)
        ...     return result
        >>> glue = Glue(input_names=['name'], output_names=['first', 'last'], function=parse_name)
        >>> glue.inputs.name='Samuel T. Brainsample'
        >>> result = glue.run()
        >>> result.outputs.first
        'Samuel'
        >>> result.outputs.middle
        'T.'
        >>> result.outputs.last
        'Brainsample'
    """
    input_spec = DynamicTraitedSpec
    
    output_spec = DynamicTraitedSpec

    def __init__(self, input_names, output_names, function=None, mandatory_inputs=True, **kwargs):
        """
        @param input_names: the input field names
        @param output_names: the output field names
        @param function: the function which sets the outputs from the inputs
        @param mandatory_inputs: a flag indicating whether every input field is required
        @param kwargs: the input field name => value bindings
        """
        
        super(Glue, self).__init__(**kwargs)
        if not input_names:
            raise Exception('Glue input fields must be a non-empty list')
        if not output_names:
            raise Exception('Glue output fields must be a non-empty list')            
        self.input_names = input_names
        self.output_names = output_names
        
        # The input trait type is Any, unless the transform is a list -> items disaggreate call. 
        trait_type = traits.Any
        # The input -> output transform.
        if function:
            self._function = function
        elif len(input_names) == len(output_names):
            self._function = _map_by_index
        elif len(output_names) == 1:
            self._function = _aggregate
        elif len(input_names) == 1:
            self._function = _disaggregate
            trait_type = traits.CList
        else:
            raise GlueError('The Glue Interface must have the same number of input fields as output fields')
        self._mandatory_inputs = mandatory_inputs

        # Adding any traits wipes out all input values set in superclass initialization,
        # even it the trait is not in the add_traits argument. The work-around is to reset
        # the values after adding the traits.
        add_traits(self.inputs, input_names, trait_type)
        self.inputs.set(**kwargs)     
        
    def _add_output_traits(self, base):
        return add_traits(base, self.output_names)

    def _run_interface(self, runtime):
        # Manual mandatory inputs check.
        if self._mandatory_inputs:
            for key in self.input_names:
                value = getattr(self.inputs, key)
                if not isdefined(value):
                    msg = "%s requires a value for input '%s' because it was listed in 'input_names'." \
                        " You can turn off mandatory inputs checking by passing mandatory_inputs = False to the constructor." % \
                        (self.__class__.__name__, key)
                    raise ValueError(msg)
        # The transform keyword arguments.
        kwargs = {key: getattr(self.inputs, key) for key in self.input_names}
        # Special built-in transforms take this Glue as an argument.
        if self._function in [_map_by_index, _aggregate, _disaggregate]:
            kwargs['interface'] = self
        # Transform the inputs to the outputs.
        xfm_result = self._function(**kwargs)
        if isinstance(xfm_result, dict):
            self._result = xfm_result
        elif is_nonstring_iterable(xfm_result) and len(xfm_result) == len(self.output_names):
            self._result = {self.output_names[i]: value for i, value in enumerate(xfm_result)}
        elif len(self.output_names) == 1:
            self._result = {self.output_names[0]: xfm_result}
        else:
            raise GlueError("The Glue result is ambiguous: %s." \
                " Return an output name => value dictionary instead." % xfm_result)
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        for key, value in self._result.iteritems():
            outputs[key] = value
        return outputs

def _map_by_index(interface, **kwargs):
    """
    Maps the input field values to the output fields based on index.

    @param kwargs: the input field name => value settings
    @return: the output field name => value dictionary
    """
    return {interface.output_names[interface.input_names.index(key)]: value for key, value in kwargs.iteritems()}

def _aggregate(interface, **kwargs):
    """
    Maps the input fields to the single output field.

    @param kwargs: the input fields
    @return: the input field value list
    """
    return {interface.output_names[0]: [getattr(interface.inputs, fld) for fld in interface.input_names]}

def _disaggregate(interface, **kwargs):
    """
    Maps the single input field to the output fields.

    @param kwargs: the single input field name => value list setting
    @return: the output field name => value settings
    """
    return {interface.output_names[i]: value for i, value in enumerate(kwargs.values()[0])}
