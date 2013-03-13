"""
Stages DICOM series directories for import into CTP.
"""

import os
from nipype.caching import Memory
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    InputMultiPath, OutputMultiPath, Directory)
from . import FixDicom, MapCTP, XNATUpload
from ..staging.staging_helpers import match_series_hierarchy

class StageCTPInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(desc='The image collection', mandatory=True)

    series_dirs = InputMultiPath(Directory(exists=True), mandatory=True,
        desc='The input series directories to stage')
    
    dest = Directory(desc='The output directory (default working directory)')


class StageCTPOutputSpec(TraitedSpec):
    series_dirs = OutputMultiPath(Directory(exists=True),
        desc='The output series directories')


class StageCTP(BaseInterface):
    input_spec = StageCTPInputSpec
    
    output_spec = StageCTPOutputSpec

    def _run_interface(self, runtime):
        opts = dict(dest=self.inputs.dest)
        self.series_dirs = self._stage(*self.inputs.series_dirs)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series_dirs'] = self.series_dirs
        return outputs

    def _stage(self, dest, *series_dirs):
        """
        Stages the given directories for import into CTP.

        @param series_dirs: the AIRC source subject directories to stage
        @return: the staged series directories
        """
        # Create a new memory context.
        mem = Memory('.')
        # The destination directory.
        dest = self.inputs.dest or os.getcwd()
        # The FixDicom nipype node factory.
        fixer = mem.cache(FixDicom)
        # The XNAT uploader.
        xnat = mem.Cache(XNATUpload)
        # The staged series directories.
        staged = []
        # The subject => DICOM file dictionary.
        sbj_files = {}
        for d in self.inputs.series_dirs:
            # Fix the input DICOM files.
            fix_result = fixer(series_dir=d, collection=self.inputs.collection)
            # Add the fixed result to the return value.
            sbj, sess, ser = match_series_hierarchy(d)
            staged.append(os.path.join(dest, sbj, sess, ser))
            # Pick one file for each subject to map.
            if not sbj_files.has_key(sbj):
                sbj_files[sbj] = fix_result.outputs.out_files[0]
        
        # Map the subject ids.
        sbj_dirs = [match_series_hierarchy(d)[0] for d in staged]
        map_result = mem.cache(MapCTP)(collection=self.inputs.collection, subject_files=sbj_files)
        # Copy the id map to the destination.
        mem.cache(Copy)(in_file=map_result.outputs.out_file, dest=dest)
        
        return staged
        
