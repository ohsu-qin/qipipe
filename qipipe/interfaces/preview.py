import os
import dicom
from matplotlib import (image, cm)
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    TraitedSpec, BaseInterface)


class PreviewInputSpec(BaseInterfaceInputSpec):
    in_file = traits.File(exists=True, mandatory=True,
                          desc='The input 3D NIfTI file')
    
    dest = traits.Directory(desc='The destination directory path'
                                 ' (default current directory)')
    
    crop = traits.List(traits.List(traits.Int),
                       desc='The x and y [min, max] bounds')
    
    out_fname = traits.File(desc='The destination file name'
                                 ' (default is the input file name)')


class PreviewOutputSpec(TraitedSpec):
    out_file = traits.File(exists=True, desc='The preview JPEG file')


class Preview(BaseInterface):
    """Preview creates a JPEG image from an input DICOM image."""
    
    input_spec = PreviewInputSpec
    
    output_spec = PreviewOutputSpec
    
    def _run_interface(self, runtime):
        self._out_file = self._convert(
            self.inputs.in_file, dest=self.inputs.dest,
            out_fname=self.inputs.out_fname, crop=self.inputs.crop
        )
        return runtime
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._out_file
        
        return outputs
    
    def _convert(self, in_file, dest=None, out_fname=None, crop=None):
        """
        Copies the given file.
        
        :param in_file: the path of the file or directory to copy
        :param dest: the destination directory path
            (default is the current directory)
        :param out_fname: the destination file name
            (default is the input file name)
        :param crop: the x and y [min, max] bounds
        :return: the preview JPEG file path
        """
        if dest:
            dest = os.path.abspath(dest)
            if not os.path.exists(dest):
                os.makedirs(dest)
        else:
            dest = os.getcwd()
        
        # The default output file name is the input file name
        # with a .jpg extension.
        if not out_fname:
            _, in_fname = os.path.split(in_file)
            base, _ = os.path.splitext(in_fname)
            out_fname = base + '.jpg'
        out_file = os.path.join(dest, out_fname)
        
        dcm = dicom.read_file(in_file)
        data = dcm.pixel_array
        # Crop the data, if necessary.
        if crop:
            crop_x, crop_y = crop
            crop_xmin, crop_xmax = crop_x
            crop_ymin, crop_ymax = crop_y
            data = data[crop_xmin:crop_xmax, crop_ymin:crop_ymax]
        image.imsave(out_file, data, cmap=cm.jet)
        
        return out_file
