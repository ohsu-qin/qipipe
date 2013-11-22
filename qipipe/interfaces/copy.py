import os
import shutil
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    TraitedSpec, BaseInterface,
                                    File, Directory)


class CopyInputSpec(BaseInterfaceInputSpec):
    in_file = traits.Either(File, Directory, exists=True, mandatory=True,
                            desc='The file or directory to copy')

    dest = Directory(mandatory=True, desc='The destination directory path')

    out_fname = traits.Either(File, Directory,
                              desc='The destination file name'
                                   ' (default is the input file name)')

class CopyOutputSpec(TraitedSpec):
    out_file = traits.Either(File, Directory, exists=True,
                             desc='The copied file or directory')


class Copy(BaseInterface):

    """The Copy interface copies a file to a destination directory."""
    input_spec = CopyInputSpec

    output_spec = CopyOutputSpec

    def _run_interface(self, runtime):
        self._out_file = self._copy(self.inputs.in_file, self.inputs.dest,
                                   self.inputs.out_fname)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._out_file

        return outputs

    def _copy(self, in_file, dest, out_fname=None):
        """
        Copies the given file.
    
        :param in_file: the path of the file or directory to copy
        :param dest: the destination directory path
        :param out_fname: the destination file name
            (default is the input file name)
        :return: the copied file path
        """
        dest = os.path.abspath(dest)
        if not os.path.exists(dest):
            os.makedirs(dest)
        if not out_fname:
            _, out_fname = os.path.split(in_file)
        out_file = os.path.join(dest, out_fname)
        if os.path.isdir(in_file):
            shutil.copytree(in_file, out_file)
        else:
            shutil.copy(in_file, out_file)
        
        return out_file
