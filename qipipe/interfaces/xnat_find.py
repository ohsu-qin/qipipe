from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    TraitedSpec, BaseInterface)
from nipype.interfaces.traits_extension import isdefined
import qixnat


class XNATFindInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')
    
    subject = traits.Str(mandatory=True, desc='The XNAT subject name')
    
    session = traits.Str(desc='The XNAT session name')
    
    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan name')
    
    reconstruction = traits.Str(desc='The XNAT reconstruction name')
    
    assessor = traits.Str(desc='The XNAT assessor name')
    
    resource = traits.Str(desc='The XNAT resource name')
    
    file = traits.Str(desc='The XNAT file name')
    
    inout = traits.Str(desc='The XNAT resource in/out designator')
    
    create = traits.Bool(default=False,
                         desc='Flag indicating whether to create the XNAT'
                              ' object if it does not yet exist')
    
    modality = traits.Str(desc="The XNAT scan modality, e.g. 'MR'")


class XNATFindOutputSpec(TraitedSpec):
    xnat_id = traits.Str(desc='The XNAT object id')


class XNATFind(BaseInterface):
    """
    The ``XNATFind`` Nipype interface wraps the ``qixnat.facade.XNAT``
    ``find_one`` and ``find_or_create`` methods.
    
    :Note: concurrent XNAT operations can fail. See the
        :class:`qipipe.pipeline.staging.StagingWorkflow` note.
    """
    
    input_spec = XNATFindInputSpec
    
    output_spec = XNATFindOutputSpec
    
    def __init__(self, **inputs):
        super(XNATFind, self).__init__(**inputs)
    
    def _run_interface(self, runtime):
        # The find options.
        opts = {}
        if self.inputs.modality:
            opts['modality'] = self.inputs.modality
        
        # The session is optional.
        if isdefined(self.inputs.session):
            session = self.inputs.session
        else:
            session = None
        
        # The resource parent.
        if self.inputs.scan:
            opts['scan'] = self.inputs.scan
        elif self.inputs.reconstruction:
            opts['reconstruction'] = self.inputs.reconstruction
        elif self.inputs.assessor:
            opts['assessor'] = self.inputs.assessor
        
        # The resource.
        if isdefined(self.inputs.resource):
            opts['resource'] = self.inputs.resource
        
        # The file.
        if isdefined(self.inputs.file):
            opts['file'] = self.inputs.file
        
        # Delegate to the XNAT helper.
        create = isdefined(self.inputs.create) and self.inputs.create
        with qixnat.connect() as xnat:
            if create:
                obj = xnat.find_or_create(self.inputs.project,
                                          self.inputs.subject, session,
                                          **opts)
            else:
                obj = xnat.find_one(self.inputs.project, self.inputs.subject,
                                    session, **opts)
            self._xnat_id = obj.id() if obj else None
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        if self._xnat_id:
            outputs['xnat_id'] = self._xnat_id
        
        return outputs
