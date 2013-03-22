import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, InputMultiPath, File)
from ..helpers.xnat_helper import XNAT


class XNATUploadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject id or label')

    session = traits.Str(mandatory=True, desc='The XNAT session id or label')

    scan = traits.Int(mandatory=True, desc='The XNAT scan id or label')

    in_files = InputMultiPath(File(exists=True), mandatory=True, desc='The files to upload')
    
    format = traits.Str(desc='The XNAT image format, optional unless the input file extension is missing')


class XNATUpload(BaseInterface):
    input_spec = XNATUploadInputSpec

    def _run_interface(self, runtime):
        opts = dict(scan=self.inputs.scan, modality='MR')
        if self.inputs.format:
            opts['format'] = self.inputs.format
        _xnat().upload(self.inputs.project, self.inputs.subject, self.inputs.session,
            *self.inputs.in_files, **opts)
        return runtime


def _xnat():
    """The XNAT facade, created on demand."""
    if not hasattr(_xnat, 'instance'):
        _xnat.instance = XNAT()
    return _xnat.instance
