from nipype.interfaces.base import (BaseInterface,
    BaseInterfaceInputSpec, traits, Directory, TraitedSpec)
from qipipe.staging.fix_dicom import fix_dicom_headers

class FixDicomInputSpec(BaseInterfaceInputSpec):
    source = Directory(desc='The input patient directory', exists=True, mandatory=True)
    dest = Directory(desc="The output location", exists=False, mandatory=True)
    collection = traits.Str(desc='The image collection', mandatory=False)


class FixDicomOutputSpec(TraitedSpec):
    target = Directory(desc="The target output patient directory", exists=True)


class FixDicom(BaseInterface):
    input_spec = FixDicomInputSpec
    output_spec = FixDicomOutputSpec

    def _run_interface(self, runtime):
        self.target = fix_dicom_headers(self.inputs.source, self.inputs.dest, self.inputs.collection)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['target'] = self.target
        return outputs
