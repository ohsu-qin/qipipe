import os
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface, File)
from ..helpers.xnat_helper import XNAT

def _xnat():
    """The XNAT connection, created on demand."""
    if not hasattr(_xnat, 'connection'):
        _xnat.connection = XNAT()
    return _xnat.connection


class XNATUploadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject id or label')

    session = traits.Str(mandatory=True, desc='The XNAT session id or label')

    scan = traits.Str(mandatory=True, desc='The XNAT scan id or label')

    in_file = File(exists=True, mandatory=True, desc='The file to upload')


class XNATUpload(BaseInterface):
    input_spec = XNATUploadInputSpec

    def _run_interface(self, runtime):
        _xnat().upload(self.inputs.project, self.inputs.subject, self.inputs.session, self.inputs.in_file, scan=self.inputs.scan, modality='MR')
        return runtime
