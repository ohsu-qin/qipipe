"""
Stages DICOM series directories for import into CTP.
"""

from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    InputMultiPath, OutputMultiPath, File, Directory)

_PROP_TMPL = 'QIN-%s-OHSU.ID-LOOKUP.properties'
"""The template for the study id map file name specified by CTP."""

class MapCTPInputSpec(BaseInterfaceInputSpec):
    subject_dirs = InputMultiPath(Directory(exists=True), mandatory=True,
        desc='The input subject DICOM directories to map')


class MapCTPOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The output properties file')


class MapCTP(BaseInterface):
    input_spec = MapCTPInputSpec
    
    output_spec = MapCTPOutputSpec

    def _run_interface(self, runtime):
        # Make the CTP id map.
        ctp_map = create_ctp_id_map(collection, first_only=True, *inputs.subject_dirs)
        # Write the id map.
        fname = _property_file(collection)
        output = open(fname, 'w')
        ctp_map.write(output)
        output.close()
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = os.path.abspath(CTP_ID_MAP)
        return outputs
    
    def _property_file(collection):
        """
        Returns the CTP id map property file name for the given collection.
        The Sarcoma collection is capitalized in the file name, Breast is not.
        """
        if collection == 'Sarcoma':
            return _PROP_TMPL % collection.upper()
        else:
            return _PROP_TMPL % collection
