from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, InputMultiPath, File)
from ..helpers import xnat_helper


class XNATUploadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(mandatory=True, desc='The XNAT session name')
    
    resource = traits.Str(desc='The XNAT resource name')
    
    format = traits.Str(desc='The XNAT image format')

    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan name')

    reconstruction = traits.Str(desc='The XNAT reconstruction name')

    assessor = traits.Str(desc='The XNAT assessor name')
    
    overwrite = traits.Bool(desc='Flag indicating whether to replace an existing file')

    in_files = InputMultiPath(File(exists=True), mandatory=True, desc='The files to upload')


class XNATUploadOutputSpec(TraitedSpec):
    xnat_files = traits.List(traits.Str, desc='The XNAT file object labels')


class XNATUpload(BaseInterface):
    """
    The ``XNATUpload`` Nipype interface wraps the
    :meth:`qipipe.helpers.xnat_helper.upload` method.
    
    :Note: only one XNAT operation can run at a time.
    """
    
    input_spec = XNATUploadInputSpec
    
    output_spec = XNATUploadOutputSpec

    def _run_interface(self, runtime):
        # The upload options.
        opts = {}
        if self.inputs.format:
            opts['format'] = self.inputs.format
        if self.inputs.resource:
            opts['resource'] = self.inputs.resource
        if self.inputs.overwrite:
            opts['overwrite'] = True
        
        # The resource parent.
        if self.inputs.scan:
            opts['modality'] = 'MR'
            opts['scan'] = self.inputs.scan
        elif self.inputs.reconstruction:
            opts['reconstruction'] = self.inputs.reconstruction
        elif self.inputs.assessor:
            opts['assessor'] = self.inputs.assessor
        
        # Upload the files.
        with xnat_helper.connection() as xnat:
            self._xnat_files = xnat.upload(self.inputs.project,
                self.inputs.subject, self.inputs.session,
                *self.inputs.in_files, **opts)
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        if hasattr(self, '_xnat_files'):
            outputs['xnat_files'] = self._xnat_files
        
        return outputs
