from nipype.interfaces.base import (BaseInterface,
    BaseInterfaceInputSpec, traits, Directory, TraitedSpec)
import os
from qipipe.staging.fix_dicom import fix_dicom_headers

class FixDicomInputSpec(BaseInterfaceInputSpec):
    source = Directory(exists=True, desc='The input patient directory', mandatory=True)
    dest = traits.String(desc='The output location', mandatory=True)


class FixDicomOutputSpec(TraitedSpec):
    out_path = Directory(exists=True, desc="The output patient directory")


class FixDicom(BaseInterface):
    input_spec = FixDicomInputSpec
    output_spec = FixDicomOutputSpec

    def _run_interface(self, runtime):
        fix_dicom_headers(self.inputs.source, self.inputs.dest)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_path'] = self.inputs.dest
        return outputs
