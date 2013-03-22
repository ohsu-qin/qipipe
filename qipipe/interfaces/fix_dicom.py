import os
from nipype.interfaces.base import (BaseInterface, BaseInterfaceInputSpec, traits,
    InputMultiPath, File, Directory, TraitedSpec)
from qipipe.staging.fix_dicom import fix_dicom_headers

class FixDicomInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(desc='The image collection', mandatory=True)

    subject = traits.Str(desc='The subject name', mandatory=True)

    in_files = InputMultiPath(File(exists=True), desc='The input DICOM files', mandatory=True)
    

class FixDicomOutputSpec(TraitedSpec):
    out_files = traits.List(desc="The modified output files", trait=File, exists=True)


class FixDicom(BaseInterface):
    input_spec = FixDicomInputSpec
    output_spec = FixDicomOutputSpec
    
    def _run_interface(self, runtime):
        self._out_files = fix_dicom_headers(self.inputs.collection, self.inputs.subject, *self.inputs.in_files)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_files'] = self._out_files
        return outputs
