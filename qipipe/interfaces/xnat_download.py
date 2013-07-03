import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, OutputMultiPath, File, Directory)
from ..helpers import xnat_helper


class XNATDownloadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(mandatory=True, desc='The XNAT session name')
    
    scan = traits.Either(traits.Str, traits.Int, desc='The XNAT scan label or number')
    
    reconstruction = traits.Str(desc='The XNAT reconstruction name')
    
    assessor = traits.Str(desc='The XNAT assessor name')
    
    format = traits.Str(desc='The XNAT image format (scan default is NIFTI')
    
    container_type = traits.Enum('scan', 'reconstruction', 'assessor',
        desc='The XNAT resource container type')
    
    dest = Directory(desc='The download location')


class XNATDownloadOutputSpec(TraitedSpec):
    out_files = traits.List(File(exists=True), desc='The downloaded files')
    out_file = File(exists=True,
        desc='The downloaded file, if exactly one file was downloaded')


class XNATDownload(BaseInterface):
    """
    The ``XNATDownload`` Nipype interface wraps the
    :meth:`qipipe.helpers.xnat_helper.download` method.
    """
    
    input_spec = XNATDownloadInputSpec
    
    output_spec = XNATDownloadOutputSpec

    def _run_interface(self, runtime):
        opts = {}
        for ctr_type in ['container_type', 'scan', 'reconstruction', 'assessor']:
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
        if len(self._out_files) == 1:
            outputs['out_file'] = self._out_files[0]
        
        return outputs
