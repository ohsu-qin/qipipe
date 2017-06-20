from nipype.interfaces.base import (traits, Undefined, isdefined)
from nipype.interfaces.io import (IOBase, add_traits)
from nipype.interfaces.utility import IdentityInterface
from .interface_error import InterfaceError


class StickyIdentityInterface(IOBase):
    """
    The StickyIdentityInterface interface class is a copy of the
    Nipype IdentityInterface. Since Nipype over-zealously elides
    IdentityInterface nodes from the execution graph,
    IdentityInterface cannot be used to capture workflow output
    nor to constrain execution order, as in the examples below.
    StickyIdentityInterface is an IdentityInterface look-alike that
    preserves the node connection in the execution graph.

    Example::

        >> from qipipe.interfaces import StickyIdentityInterface
        >> gate = Node(StickyIdentityInterface(fields=['a', 'b']))
        >> workflow.connect(upstream1, 'a', gate, 'a')
        >> workflow.connect(upstream2, 'b', gate, 'b')
        >> workflow.connect(gate, 'a', downstream, 'a')

    In this example, the ``gate`` node starts after both ``upstream1`` and
    ``upstream2`` finish. Consequently, the ``downstream`` node starts only
    after both ``upstream1`` and ``upstream2`` finish. This execution
    precedence constraint does not hold if gate were an IdentityInterface.

    Example::

        >> from qipipe.interfaces import StickyIdentityInterface
        >> output_spec = Node(StickyIdentityInterface(fields=['a']))
        >> workflow.connect(upstream, 'a', output_spec, 'a')
        >> upstream.inputs.a = 1
        >> # The magic incantation to get a Nipype workflow output.
        >> wf_res = workflow.run()
        >> output_res = next(n for n in wf_res.nodes()
        ...                  if n.name == 'output_spec')
        >> output_res.inputs.get()['a']
        1
        >> # But, oddly:
        >> output_res.outputs.get()['a']  # bad!
        <undefined>
    
    .. Note:: a better solution is to set a *preserve* flag on
        IdentityInterface. If this solution is implemented by Nipype,
        then this :class:`StickyIdentityInterface` class will be
        deprecated.
    """
    input_spec = IdentityInterface.input_spec
    output_spec = IdentityInterface.output_spec

    def __init__(self, fields=None, mandatory_inputs=True, **inputs):
        super(StickyIdentityInterface, self).__init__(**inputs)
        if fields is None or not fields:
            raise InterfaceError('StickyIdentityInterface fields must be a non-empty list')
        # Each input must be in the fields.
        for in_field in inputs:
            if in_field not in fields:
                raise InterfaceError('StickyIdentityInterface input is not in the fields: %s' % in_field)
        self._fields = fields
        self._mandatory_inputs = mandatory_inputs
        add_traits(self.inputs, fields)
        # Adding any traits wipes out all input values set in superclass initialization,
        # even it the trait is not in the add_traits argument. The work-around is to reset
        # the values after adding the traits.
        self.inputs.set(**inputs)

    def _add_output_traits(self, base):
        undefined_traits = {}
        for key in self._fields:
            base.add_trait(key, traits.Any)
            undefined_traits[key] = Undefined
        base.trait_set(trait_change_notify=False, **undefined_traits)
        return base

    def _list_outputs(self):
        #manual mandatory inputs check
        if self._fields and self._mandatory_inputs:
            for key in self._fields:
                value = getattr(self.inputs, key)
                if not isdefined(value):
                    msg = "%s requires a value for input '%s' because it was listed in 'fields'. \
                    You can turn off mandatory inputs checking by passing mandatory_inputs = False to the constructor." % \
                    (self.__class__.__name__, key)
                    raise InterfaceError(msg)

        outputs = self._outputs().get()
        for key in self._fields:
            val = getattr(self.inputs, key)
            if isdefined(val):
                outputs[key] = val
        return outputs
