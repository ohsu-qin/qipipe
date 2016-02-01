from nipype.interfaces.base import (
    BaseInterface, BaseInterfaceInputSpec, traits,
    File, TraitedSpec)
from qipipe.staging.staging_error import StagingError
from qipipe.staging.fix_dicom import fix_dicom_headers


class FixDicomInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(desc='The image collection', mandatory=True)
    
    subject = traits.Str(desc='The subject name', mandatory=True)
    
    in_file = File(exists=True, desc='The input DICOM file', mandatory=True)


class FixDicomOutputSpec(TraitedSpec):
    out_file = File(desc="The modified output file", exists=True)


class FixDicom(BaseInterface):
    """
    The FixDicom interface wraps the
    :meth:`qipipe.staging.fix_dicom.fix_dicom_headers` function.
    """
    
    input_spec = FixDicomInputSpec
    
    output_spec = FixDicomOutputSpec
    
    def _run_interface(self, runtime):
        fixed = fix_dicom_headers(self.inputs.collection, self.inputs.subject,
                                  self.inputs.in_file)
        if len(fixed) != 1:
            raise StagingError("Fixed DICOM file count is not one: %s" % fixed)
        self._out_file = fixed[0]
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._out_file
        
        return outputs
