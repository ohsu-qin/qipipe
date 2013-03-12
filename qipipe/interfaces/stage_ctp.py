"""
Stages DICOM series directories for import into CTP.
"""

from nipype.caching import Memory
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    InputMultiPath, OutputMultiPath, Directory)
from . import GroupDicom, FixDicom, Copy
from ..staging.staging_helpers import match_session_hierarchy

class StageCTPInputSpec(BaseInterfaceInputSpec):
    collection = traits.Str(desc='The image collection', mandatory=True)

    subject_dirs = InputMultiPath(Directory(exists=True), mandatory=True,
        desc='The input subject directories to stage')
    
    dest = Directory(desc='The output directory (default working directory)')


class StageCTPOutputSpec(TraitedSpec):
    series_dirs = OutputMultiPath(Directory(exists=True),
        desc='The output series directories')


class StageCTP(BaseInterface):
    input_spec = StageCTPInputSpec
    
    output_spec = StageCTPOutputSpec

    def _run_interface(self, runtime):
        opts = dict(dest=self.inputs.dest)
        self.series_dirs = _stage(*self.inputs.subject_dirs)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series_dirs'] = self.series_dirs
        return outputs


    def _stage(self, dest, *subject_dirs):
        """
        Stages the given directories for import into CTP.

        @param subject_dirs: the AIRC source subject directories to stage
        @return: the staged series directories
        """
        # Create a new memory context.
        mem = Memory('.')
        # The destination directory.
        dest = self.inputs.dest or os.getcwd()
        # The FixDicom nipype node factory.
        fixer = mem.cache(FixDicom)
        # The Move nipype node factory.
        mover = mem.cache(Move)
        for d in self.input.subject_dirs:
            fixed = fixer(source=d, dest=dest, collection=self.inputs.collection)
            for f in fixed.outputs.out_files:
                # The subject, session and series directory components.
                sbj, sess, ser = match_session_hierarchy(f)
                # The copy target directory.
                tgt = os.path.join(self.ctp_dir, sbj, sess, ser)
                # Move the file to the target series directory.
                mover(in_file=f, dest=tgt)
                # Add the move target series directory to the staged set.
                staged.add(tgt)
        return staged
