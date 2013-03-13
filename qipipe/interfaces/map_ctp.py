"""
Stages DICOM series directories for import into CTP.
"""

import os
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    InputMultiPath, OutputMultiPath, File, Directory)
from ..staging import CTPPatientIdMap


class MapCTPInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(mandatory=True, desc='The collection name')
    
    subject_files = traits.Dict(
        traits.Str(desc='The canonical OHSU subject id'),
        InputMultiPath(File(exists=True), desc='The DICOM files to map'),
        mandatory=True,
        desc="The subject id => DICOM files dictionary")


class MapCTPOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The output properties file')


class MapCTP(BaseInterface):
    PROP_TMPL = 'QIN-%s-OHSU.ID-LOOKUP.properties'
    """The template for the study id map file name specified by CTP."""

    input_spec = MapCTPInputSpec
    
    output_spec = MapCTPOutputSpec
    
    def _run_interface(self, runtime):
        # Make the CTP id map.
        ctp_map = CTPPatientIdMap()
        for sbj_id, dicom_files in self.inputs.subject_files.iteritems():
            ctp_map.map_dicom_files(sbj_id, *dicom_files)
        # Write the id map.
        self.out_file = self._property_file(self.inputs.collection)
        output = open(self.out_file, 'w')
        ctp_map.write(output)
        output.close()
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = os.path.abspath(self.out_file)
        return outputs
    
    def _property_file(self, collection):
        """
        Returns the CTP id map property file name for the given collection.
        The Sarcoma collection is capitalized in the file name, Breast is not.
        """
        if collection == 'Sarcoma':
            return MapCTP.PROP_TMPL % collection.upper()
        else:
            return MapCTP.PROP_TMPL % collection
