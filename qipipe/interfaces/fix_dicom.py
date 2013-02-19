import os
from nipype.interfaces.base import (BaseInterface,
    BaseInterfaceInputSpec, traits, File, Directory, TraitedSpec)
from qipipe.staging.fix_dicom import fix_dicom_headers

class FixDicomInputSpec(BaseInterfaceInputSpec):
    source = Directory(desc='The input patient directory', exists=True, mandatory=False)
    dest = Directory(desc="The output location", exists=False, mandatory=True)
    collection = traits.Str(desc='The image collection', mandatory=True)


class FixDicomOutputSpec(TraitedSpec):
    out_files = traits.List(desc="The modified output files", trait=File, exists=True)


class FixDicom(BaseInterface):
    input_spec = FixDicomInputSpec
    output_spec = FixDicomOutputSpec
    
    def _run_interface(self, runtime):
        files = fix_dicom_headers(self.inputs.source, self.inputs.dest, self.inputs.collection)
        self._out_files = [os.path.abspath(f) for f in files]
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_files'] = self._out_files
        return outputs
