import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, OutputMultiPath, File, Directory)
from ..helpers import xnat_helper


class XNATDownloadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(mandatory=True, desc='The XNAT session name')
    
    scan = traits.Either(traits.Str, traits.Int, desc='The XNAT scan resource container name')
    
    reconstruction = traits.Str(desc='The XNAT reconstruction resource container name')
    
    analysis = traits.Str(desc='The XNAT assessment resource container name')
    
    format = traits.Enum('NIFTI', 'DICOM', desc='The XNAT image format (default NIFTI)')
    
    dest = Directory(desc='The download location')


class XNATDownloadOutputSpec(TraitedSpec):
    out_files = OutputMultiPath(File(exists=True), desc='The downloaded files')


class XNATDownload(BaseInterface):
    input_spec = XNATDownloadInputSpec
    
    output_spec = XNATDownloadOutputSpec

    def _run_interface(self, runtime):
        opts = {}
        for ctr_type in ['scan', 'reconstruction', 'analysis']:
            ctr_name = getattr(self.inputs, ctr_type)
            if ctr_name:
                opts[ctr_type] = ctr_name
                break
        if self.inputs.dest:
            opts['dest'] = self.inputs.dest
        if self.inputs.format:
            opts['format'] = self.inputs.format
        with xnat_helper.connection() as xnat:
            self._out_files = xnat.download(self.inputs.project,
                self.inputs.subject, self.inputs.session, **opts)
        
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_files'] = self._out_files
        
        return outputs
