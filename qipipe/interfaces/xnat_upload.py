## Deprecated - see XNATUpload comment. ##

from nipype.interfaces.base import (
    traits, BaseInterfaceInputSpec, TraitedSpec,
    BaseInterface, InputMultiPath, File)
import qixnat


class XNATUploadInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')
    
    subject = traits.Str(mandatory=True, desc='The XNAT subject name')
    
    session = traits.Str(desc='The XNAT session name')
    
    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan name')
    
    reconstruction = traits.Str(desc='The XNAT reconstruction name')
    
    assessor = traits.Str(desc='The XNAT assessor name')
    
    resource = traits.Str(mandatory=True, desc='The XNAT resource name')
    
    inout = traits.Str(desc='The XNAT reconstruction or assessor resource'
                            ' in/out qualifier')
    
    force = traits.Bool(desc='Flag indicating whether to replace an existing'
                             ' XNAT file')
    
    skip_existing = traits.Bool(desc='Flag indicating whether to skip upload'
                                     ' to an existing target XNAT file')
    
    in_files = InputMultiPath(File(exists=True), mandatory=True,
                              desc='The files to upload')
    
    modality = traits.Str(desc="The XNAT scan modality, e.g. 'MR'")


class XNATUploadOutputSpec(TraitedSpec):
    xnat_files = traits.List(traits.Str, desc='The XNAT file object labels')


class XNATUpload(BaseInterface):
    """
    The ``XNATUpload`` Nipype interface wraps the
    :meth:`qixnat.facade.XNAT.upload` method.
    """
    
    input_spec = XNATUploadInputSpec
    
    output_spec = XNATUploadOutputSpec
    
    def _run_interface(self, runtime):
        # The upload options.
        find_opts = {}
        if self.inputs.resource:
            find_opts['resource'] = self.inputs.resource
        if self.inputs.inout:
            find_opts['inout'] = self.inputs.inout
        if self.inputs.modality:
            find_opts['modality'] = self.inputs.modality
        if self.inputs.scan:
            find_opts['scan'] = self.inputs.scan
        elif self.inputs.reconstruction:
            find_opts['reconstruction'] = self.inputs.reconstruction
        elif self.inputs.assessor:
            find_opts['assessor'] = self.inputs.assessor
        upload_opts = {}
        if self.inputs.force:
            upload_opts['force'] = True
        if self.inputs.skip_existing:
            upload_opts['skip_existing'] = True
        
        # Upload the files.
        with qixnat.connect() as xnat:
            # The XNAT scan resource object.
            rsc = xnat.find_or_create(self.inputs.project, self.inputs.subject,
                                      self.inputs.session, **find_opts)
            self._xnat_files = xnat.upload(rsc, *self.inputs.in_files,
                                           **upload_opts)
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        if hasattr(self, '_xnat_files'):
            outputs['xnat_files'] = self._xnat_files
        
        return outputs
