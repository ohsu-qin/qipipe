import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, OutputMultiPath, File, Directory)
from ..helpers import xnat_helper


class XNATDownloadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(mandatory=True, desc='The XNAT session name')
    
    container_type = traits.Str(mandatory=True, desc='The XNAT resource container type, e.g. scan or reconstruction')
    
    format = traits.Str(mandatory=True, desc='The XNAT image format (NIFIT or DICOM)')
    
    dest = Directory(desc='The download location')


class XNATDownloadOutputSpec(TraitedSpec):
    out_files = OutputMultiPath(File(exists=True), desc='The downloaded files')
    
    format = traits.Str(desc='The XNAT image format, optional unless the input file extension is missing')


class XNATDownload(BaseInterface):
    input_spec = XNATDownloadInputSpec
    
    output_spec = XNATDownloadOutputSpec

    def _run_interface(self, runtime):
        with xnat_helper.connection() as xnat:
            self._out_files = xnat.download(self.inputs.project, self.inputs.subject,
                self.inputs.session, format=self.inputs.format, container_type=self.inputs.container_type,
                dest=self.inputs.dest)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_files'] = self._out_files
        return outputs
