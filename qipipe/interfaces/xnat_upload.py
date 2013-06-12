import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, InputMultiPath, File)
from ..helpers import xnat_helper


class XNATUploadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(mandatory=True, desc='The XNAT session name')
    
    format = traits.Str(desc='The XNAT image format')

    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan name')

    reconstruction = traits.Str(desc='The XNAT reconstruction name')

    analysis = traits.Str(desc='The (XNAT assessor) name')

    in_files = InputMultiPath(File(exists=True), mandatory=True, desc='The files to upload')


class XNATUpload(BaseInterface):
    input_spec = XNATUploadInputSpec

    def _run_interface(self, runtime):
        opts = dict(modality='MR', format=self.inputs.format)
        # The resource parent.
        if self.inputs.scan:
            opts['scan'] = self.inputs.scan
        elif self.inputs.reconstruction:
            opts['reconstruction'] = self.inputs.reconstruction
        elif self.inputs.analysis:
            opts['analysis'] = self.inputs.analysis
        with xnat_helper.connection() as xnat:
            xnat.upload(self.inputs.project, self.inputs.subject, self.inputs.session,
                *self.inputs.in_files, **opts)
        
        return runtime
