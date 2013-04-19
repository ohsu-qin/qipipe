import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, InputMultiPath, File)
from ..helpers import xnat_helper


class XNATUploadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject id or label')

    session = traits.Str(mandatory=True, desc='The XNAT session id or label')
    
    format = traits.Str(mandatory=True, desc='The XNAT image format')

    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan id or label')

    reconstruction = traits.Either(traits.Int, traits.Str, desc='The XNAT reconstruction id or label')

    assessor = traits.Either(traits.Int, traits.Str, desc='The XNAT assessor id or label')

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
        elif self.inputs.assessor:
            opts['assessor'] = self.inputs.assessor
        xnat_helper.facade().upload(self.inputs.project, self.inputs.subject, self.inputs.session,
            *self.inputs.in_files, **opts)
        return runtime
