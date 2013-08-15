import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, InputMultiPath, File)
from nipype.interfaces.traits_extension import isdefined
from ..helpers import xnat_helper


class XNATFindInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(desc='The XNAT session name')

    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan name')

    reconstruction = traits.Str(desc='The XNAT reconstruction name')

    assessor = traits.Str(desc='The XNAT assessor name')

    resource = traits.Str(desc='The XNAT resource name')

    inout = traits.Str(desc='The XNAT resource in/out designator')
    
    create = traits.Bool(default=False, desc='Flag indicating whether to '
        'create the XNAT object if it does not yet exist')


class XNATFindOutputSpec(TraitedSpec):
    xnat_id = traits.Str(desc='The XNAT object id')


class XNATFind(BaseInterface):
    """
    The ``XNATFind`` Nipype interface wraps the
    :meth:`qipipe.helpers.xnat_helper.find` method.
    
    :Note: only one XNAT operation can run at a time.
    """
    
    input_spec = XNATFindInputSpec
    
    output_spec = XNATFindOutputSpec

    def __init__(self, **inputs):
        super(XNATFind, self).__init__(**inputs)

    def _run_interface(self, runtime):
        # The find options.
        opts = dict(create=self.inputs.create)
        
        # The session is optional.
        if isdefined(self.inputs.session):
            session = self.inputs.session
        else:
            session = None
        
        # The resource parent.
        if self.inputs.scan:
            opts['modality'] = 'MR'
            opts['scan'] = self.inputs.scan
        elif self.inputs.reconstruction:
            opts['reconstruction'] = self.inputs.reconstruction
        elif self.inputs.assessor:
            opts['assessor'] = self.inputs.assessor
        
        # The resource.
        if isdefined(self.inputs.resource):
            opts['resource'] = self.inputs.resource
        
        # Delegate to the XNAT helper.
        with xnat_helper.connection() as xnat:
            obj = xnat.find(self.inputs.project, self.inputs.subject,
                session, **opts)
            if obj and (opts['create'] or obj.exists()):
                self._xnat_id = obj.id()
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        if hasattr(self, '_xnat_id'):
            outputs['xnat_id'] = self._xnat_id
        
        return outputs
