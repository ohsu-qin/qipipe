from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    InputMultiPath, OutputMultiPath, Directory, File)
from qipipe.staging.staging_helper import group_dicom_files_by_series


class GroupDicomInputSpec(BaseInterfaceInputSpec):
    in_files = InputMultiPath(traits.Either(File(exists=True), Directory(exists=True)),
        mandatory=True, desc='The DICOM files to group')


class GroupDicomOutputSpec(TraitedSpec):
    series = traits.Str(desc='The series numbers')
    
    out_files = OutputMultiPath(File(exists=True), desc='The series DICOM files')


class GroupDicom(BaseInterface):
    input_spec = GroupDicomInputSpec
    
    output_spec = GroupDicomOutputSpec

    def _run_interface(self, runtime):
        self.grp_dict = group_dicom_files_by_series(self.inputs.in_files)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series'] = self.grp_dict.keys()
        outputs['out_files'] = self.grp_dict.values()
        return outputs
     