import os
import shutil
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    TraitedSpec, BaseInterface,
                                    File, Directory)


class MoveInputSpec(BaseInterfaceInputSpec):
    in_file = traits.Either(File, Directory, exists=True, mandatory=True,
                            desc='The file or directory to move')

    dest = traits.Either(File, Directory, mandatory=True,
                         desc='The destination path')


class MoveOutputSpec(TraitedSpec):
    out_file = traits.Either(
        File, Directory, exists=True, desc='The moved file or directory')


class Move(BaseInterface):
    """
    The Move interface moves a file to a destination using
    ``shutil.move``. Unlike ``shutil.move``, the *dest* parent
    directory is created if it does not yet exist
    (like ``mkdir -p``).
    """

    input_spec = MoveInputSpec

    output_spec = MoveOutputSpec

    def _run_interface(self, runtime):
        self.out_file = self._move(self.inputs.in_file, self.inputs.dest)

        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self.out_file

        return outputs

    def _move(self, in_file, dest):
        """
        Moves the given file.

        :param in_file: the path of the file to move
        @parma dest: the destination file or directory path
        :return: the moved file path
        """
        dest = os.path.abspath(dest)
        dest_parent_dir, dest_base_name = os.path.split(dest)
        if os.path.exists(dest):
            out_file = os.path.join(dest, dest_base_name)
        else:
            if not os.path.exists(dest_parent_dir):
                os.makedirs(dest_parent_dir)
            out_file = dest
        shutil.move(in_file, dest)

        return out_file
