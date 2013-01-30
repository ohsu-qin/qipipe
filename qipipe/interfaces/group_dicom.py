from nipype.interfaces.base import (BaseInterface,
    BaseInterfaceInputSpec, traits, Directory, TraitedSpec)
import os
from qipipe.staging.group_dicom import group_dicom_files

class GroupDicomInputSpec(BaseInterfaceInputSpec):
    source = Directory(desc='The input patient directory', exists=True, mandatory=True)
    dest = Directory(desc="The output location", exists=False, mandatory=True)
    delta = Directory(desc='The delta directory holding links to new visits', exists=True, mandatory=False)

class GroupDicomOutputSpec(TraitedSpec):
    series_dirs = traits.List(desc="The output series directories", exists=False)


class GroupDicom(BaseInterface):
    input_spec = GroupDicomInputSpec
    output_spec = GroupDicomOutputSpec

    def _run_interface(self, runtime):
        self._series_dirs = group_dicom_files(self.inputs.source, target=self.inputs.dest)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series_dirs'] = self._series_dirs
        return outputs
