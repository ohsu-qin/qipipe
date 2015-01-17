from nipype.interfaces.base import (traits,
                                    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
                                    InputMultiPath, OutputMultiPath, Directory, File)
from qidicom.hierarchy import group_dicom_files_by_series


class GroupDicomInputSpec(BaseInterfaceInputSpec):
    in_files = InputMultiPath(
        traits.Either(File(exists=True), Directory(exists=True)),
        mandatory=True, desc='The DICOM files to group')


class GroupDicomOutputSpec(TraitedSpec):
    series_files_dict = traits.Dict(desc='The series number: [DICOM files] dictionary')


class GroupDicom(BaseInterface):
    input_spec = GroupDicomInputSpec

    output_spec = GroupDicomOutputSpec

    def _run_interface(self, runtime):
        self.grp_dict = group_dicom_files_by_series(*self.inputs.in_files)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series_files_dict'] = self.grp_dict
        return outputs
