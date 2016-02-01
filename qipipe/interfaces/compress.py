import os
import gzip
from nipype.interfaces.base import (BaseInterfaceInputSpec, TraitedSpec,
                                    BaseInterface, File, Directory)


class CompressInputSpec(BaseInterfaceInputSpec):
    in_file = File(exists=True, mandatory=True, desc='The file to compress')
    
    dest = Directory(desc='The optional directory to write the compressed file'
                     '(default current directory)')


class CompressOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc='The compressed file')


class Compress(BaseInterface):
    input_spec = CompressInputSpec
    
    output_spec = CompressOutputSpec
    
    def _run_interface(self, runtime):
        self.out_file = self._compress(
            self.inputs.in_file, dest=self.inputs.dest)
        
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self.out_file
        
        return outputs
    
    def _compress(self, in_file, dest=None):
        """
        Compresses the given file.
        
        :param in_file: the path of the file to compress
        :param dest: the destination (default is the working directory)
        :return: the compressed file path
        """
        if not dest:
            dest = os.getcwd()
        if not os.path.exists(dest):
            os.makedirs(dest)
        _, fname = os.path.split(in_file)
        out_file = os.path.join(dest, fname + '.gz')
        f = open(in_file, 'rb')
        cf = gzip.open(out_file, 'wb')
        cf.writelines(f)
        f.close()
        cf.close()
        
        return os.path.abspath(out_file)
