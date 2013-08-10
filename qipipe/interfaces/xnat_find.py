import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, InputMultiPath, File)
from nipype.interfaces.traits_extension import isdefined
from pyxnat.core.resources import (Reconstruction, Assessor)
from ..helpers import xnat_helper


class XNATFindInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(desc='The XNAT session name')

    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan name')

    reconstruction = traits.Str(desc='The XNAT reconstruction name')

    assessor = traits.Str(desc='The XNAT assessor name')
    
    create = traits.Bool(default=False, desc='Flag indicating whether to '
        'create the XNAT object if it does not yet exist')


class XNATFindOutputSpec(TraitedSpec):
    label = traits.Str(desc='The XNAT object label')


class XNATFind(BaseInterface):
    """
    The ``XNATFind`` Nipype interface wraps the
    :meth:`qipipe.helpers.xnat_helper.find` method.
    """
    
    input_spec = XNATFindInputSpec
    
    output_spec = XNATFindOutputSpec

    def __init__(self, **inputs):
        super(XNATFind, self).__init__(**inputs)

    def _run_interface(self, runtime):
        # The find options.
        opts = dict(create=self.inputs.create)
        
        # The resource parent.
        if self.inputs.scan:
            opts['modality'] = 'MR'
            opts['scan'] = self.inputs.scan
        elif self.inputs.reconstruction:
            opts['reconstruction'] = self.inputs.reconstruction
        elif self.inputs.assessor:
            opts['assessor'] = self.inputs.assessor
        
        # The session is optional.
        if isdefined(self.inputs.session):
            session = self.inputs.session
        else:
            session = None
        
        # Delegate to the XNAT helper.
        with xnat_helper.connection() as xnat:
            obj = xnat.find(self.inputs.project, self.inputs.subject,
                session, **opts)
            if obj and (opts['create'] or obj.exists()):
                if isinstance(obj, Assessor) or isinstance(obj, Reconstruction):
                    self._label = obj.id()
                else:
                    self._label = obj.label()
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        if hasattr(self, '_label'):
            outputs['label'] = self._label
        return outputs
