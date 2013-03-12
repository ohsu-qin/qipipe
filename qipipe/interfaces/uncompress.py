import os, gzip
from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    File)

class UncompressInputSpec(BaseInterfaceInputSpec):
    in_file = File(exists=True, mandatory=True,
        desc='The file to uncompress')


class UncompressOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The uncompressed file')


class Uncompress(BaseInterface):
    input_spec = UncompressInputSpec
    
    output_spec = UncompressOutputSpec

    def _run_interface(self, runtime):
        self.out_file = _uncompress(self.inputs.in_file)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self.out_file
        return outputs

    def _uncompress(in_file, dest=None):
        """
        Uncompresses the given file.
    
        @param in_file: the path of the file to uncompress
        @parma dest: the destination (default is the working directory)
        @return: the compressed file path
        """
        if not dest:
            dest = os.getcwd()
        if not os.path.exists(dest):
            os.makedirs(dest)
        _, fname = os.path.split(in_file)
        out_file = os.path.join(dest, fname[:-3])
        cf = gzip.open(in_file, 'rb')
        f = open(out_file, 'wb')
        f.writelines(cf)
        f.close()
        cf.close()
        return os.path.abspath(out_file)