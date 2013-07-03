import os
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

    in_files = InputMultiPath(File(exists=True), mandatory=True, desc='The files to upload')


class XNATUpload(BaseInterface):
    """
    The ``XNATUpload`` Nipype interface wraps the
    :meth:`qipipe.helpers.xnat_helper.upload` method.
    """
    
    input_spec = XNATUploadInputSpec

    def _run_interface(self, runtime):
        # The upload options.
        opts = {}
        if self.inputs.format:
            opts['format'] = self.inputs.format
        if self.inputs.resource:
            opts['resource'] = self.inputs.resource
        
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
            xnat.upload(self.inputs.project, self.inputs.subject, self.inputs.session,
                *self.inputs.in_files, **opts)
        
        return runtime
