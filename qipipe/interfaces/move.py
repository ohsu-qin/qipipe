import os, shutil
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    File, Directory)


class MoveInputSpec(BaseInterfaceInputSpec):
    in_file = traits.Either(File, Directory, exists=True, mandatory=True, desc='The file or directory to move')

    dest = traits.Either(File, Directory, mandatory=True, desc='The destination path')


class MoveOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The moved file or directory')


class Move(BaseInterface):
    input_spec = MoveInputSpec
    
    output_spec = MoveOutputSpec

    def _run_interface(self, runtime):
        self.out_file = _move(self.inputs.in_file)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self.out_file
        return outputs

    def _move(in_file, dest):
        """
        Moves the given file.
    
        @param in_file: the path of the file to move
        @parma dest: the destination directory path
        @return: the moved file path
        """
        dest = os.path.abspath(dest)
        if not os.path.exists(dest):
            os.makedirs(dest)
        shutil.move(in_file, dest)
        _, fname = os.path.split(in_file)
        out_file = os.path.join(dest, fname)
        return out_file