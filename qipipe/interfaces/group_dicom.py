from nipype.interfaces.base import (traits,
                                    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
                                    InputMultiPath, OutputMultiPath, Directory, File)
from qidicom import hierarchy


class GroupDicomInputSpec(BaseInterfaceInputSpec):
    tag = traits.String(mandatory=True)
    
    in_files = InputMultiPath(
        traits.Either(File(exists=True), Directory(exists=True)),
        mandatory=True, desc='The DICOM files to group')


class GroupDicomOutputSpec(TraitedSpec):
    series_files_dict = traits.Dict(desc='The series number: [DICOM files] dictionary')


class GroupDicom(BaseInterface):
    input_spec = GroupDicomInputSpec

    output_spec = GroupDicomOutputSpec

    def _run_interface(self, runtime):
        self.grp_dict = hierarchy.group_by(self.inputs.tag, *self.inputs.in_files)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series_files_dict'] = self.grp_dict
        return outputs
