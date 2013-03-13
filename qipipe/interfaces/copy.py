import os, shutil
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    File, Directory)


class CopyInputSpec(BaseInterfaceInputSpec):
    in_file = traits.Either(File, Directory, exists=True, mandatory=True, desc='The file or directory to copy')

    dest = traits.Either(File, Directory, mandatory=True, desc='The destination path')


class CopyOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The copied file or directory')


class Copy(BaseInterface):
    input_spec = CopyInputSpec
    
    output_spec = CopyOutputSpec

    def _run_interface(self, runtime):
        self.out_file = self._copy(self.inputs.in_file, self.inputs.dest)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self.out_file
        return outputs

    def _copy(self, in_file, dest):
        """
        Copys the given file.
    
        @param in_file: the path of the file to copy
        @parma dest: the destination directory path
        @return: the copyd file path
        """
        dest = os.path.abspath(dest)
        if not os.path.exists(dest):
            os.makedirs(dest)
        shutil.copy(in_file, dest)
        _, fname = os.path.split(in_file)
        out_file = os.path.join(dest, fname)
        return out_file
