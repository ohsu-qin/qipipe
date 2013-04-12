"""
Maps the OHSU DICOM Patient IDs to the CTP Patient IDs.
"""

import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
    TraitedSpec, BaseInterface, File, Directory)
from ..staging.map_ctp import property_filename, CTPPatientIdMap


class MapCTPInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(mandatory=True, desc='The collection name')
    
    patient_ids = traits.CList(traits.Str(), mandatory=True,
        desc='The DICOM Patient IDs to map')

    dest = Directory(desc='The optional directory to write the map file (default current directory)')


class MapCTPOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The output properties file')


class MapCTP(BaseInterface):

    input_spec = MapCTPInputSpec
    
    output_spec = MapCTPOutputSpec
    
    def _run_interface(self, runtime):
        # Make the CTP id map.
        ctp_map = CTPPatientIdMap()
        ctp_map.add_subjects(self.inputs.collection, *self.inputs.patient_ids)
        # Write the id map property file.
        if self.inputs.dest:
            dest = self.inputs.dest
            if not os.path.exists(dest):
                os.makedirs(dest)
        else:
            dest = os.getcwd()
        self.out_file = os.path.join(dest, property_filename(self.inputs.collection))
        output = open(self.out_file, 'w')
        ctp_map.write(output)
        output.close()
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = os.path.abspath(self.out_file)
        return outputs