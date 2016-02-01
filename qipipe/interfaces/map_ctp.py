"""
Maps the DICOM Patient IDs to the CTP Patient IDs.
"""

import os
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    TraitedSpec, BaseInterface, File,
                                    Directory)
from ..staging.map_ctp import map_ctp


class MapCTPInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(mandatory=True, desc='The collection name')
    
    subjects = traits.CList(traits.Str(), mandatory=True,
                               desc='The DICOM Patient IDs to map')
    
    dest = Directory(desc='The optional directory to write the map file '
                     '(default current directory)')


class MapCTPOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The output properties file')


class MapCTP(BaseInterface):
    """
    The MapCTP interface wraps the
    :meth:`qipipe.interfaces.map_ctp.map_ctp` method.
    """
    
    input_spec = MapCTPInputSpec
    
    output_spec = MapCTPOutputSpec
    
    def _run_interface(self, runtime):
        self._out_file = map_ctp(self.inputs.collection, *self.inputs.subjects,
                                 dest=self.inputs.dest)
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._out_file
        return outputs
