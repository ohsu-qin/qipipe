from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    InputMultiPath, OutputMultiPath, Directory)
from qipipe.staging.group_dicom import group_dicom_files


class GroupDicomInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(mandatory=True, desc='The collection name')
    
    subject_dirs = InputMultiPath(Directory(exists=True), mandatory=True,
        desc='The input subject directories to group')
    
    dest = Directory(exists=False, desc='The output directory')
    
    dicom_pat = traits.Str(desc='The DICOM file glob pattern')
    
    session_pat = traits.Str(desc='The session subdirectory glob pattern')


class GroupDicomOutputSpec(TraitedSpec):
    series_dirs = OutputMultiPath(Directory(exists=True),
        desc='The output series directories')


class GroupDicom(BaseInterface):
    input_spec = GroupDicomInputSpec
    
    output_spec = GroupDicomOutputSpec

    def _run_interface(self, runtime):
        opts = dict(dest=self.inputs.dest)
        if self.inputs.dicom_pat:
            opts['dicom_pat'] = self.inputs.dicom_pat
        if self.inputs.session_pat:
            opts['session_pat'] = self.inputs.session_pat
        self.series_dirs = group_dicom_files(self.inputs.collection, *self.inputs.subject_dirs, **opts)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series_dirs'] = self.series_dirs
        return outputs
     