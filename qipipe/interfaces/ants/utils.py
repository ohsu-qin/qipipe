# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft = python sts = 4 ts = 4 sw = 4 et:
"""ANTS Affine Initializer interface
   
   Change directory to provide relative paths for doctests
   >>> import os
   >>> filepath = os.path.dirname( os.path.realpath( __file__ ) )
   >>> datadir = os.path.realpath(os.path.join(filepath,
   ...                                         '../../../test/fixtures/staged/'
   ...                                         'breast/Breast003/Session01/'
   ...                                         'scans/1/resources'))
   >>> os.chdir(datadir)

"""
import os

from nipype.interfaces.ants.base import (ANTSCommand, ANTSCommandInputSpec)
from nipype.interfaces.base import (TraitedSpec, File, traits)
from nipype.interfaces.base import InputMultiPath


class AffineInitializerInputSpec(ANTSCommandInputSpec):
    """
    :Note: The antsAffineInitializer command line options are positional
        rather than keyword, e.g.::
            
            antsAffineInitializer 3 fixed.nii.gz moving.nii.gz initial_xform.mat \
            10 0.3
        
        rather than::
            
            antsAffineInitializer 3 fixed.nii.gz moving.nii.gz initial_xform.mat \
            --search_factor 10 --radian_fraction 0.3
        
        Therefore, each optional input field requires the prior input field,
        e.g. *radian_fraction* requires a *search_factor* input. Reasonable
        starting option values are shown in the :class:`AffineInitializer`
        example.
        
        In particular, note that since an image mask is the last positional
        argument, if the *image_mask* input option is set, then all other
        options must be set as well.
    """
    dimension = traits.Enum(3, 2, argstr='%d', usedefault=False,
                            mandatory=True, position=0,
                            desc='image dimension (2 or 3)')
    fixed_image = File(argstr='%s', exists=True, mandatory=True, position=1,
                       desc='realignment reference image')
    moving_image = File(argstr='%s', exists=True, mandatory=True, position=2,
                        desc='image to be realigned')
    output_affine_transform = File(argstr='%s', mandatory=True, position=3,
                                   desc='the name of the resulting transform')
    search_factor = traits.Int(argstr='%d', position=4,
                               desc='search increments in degrees')
    radian_fraction = traits.Float(argstr='%f', position=5,
                                   requires=['search_factor'],
                                   desc='search fraction about the principal'
                                        ' axis, between 0 and 1')
    use_principal_axes = traits.Bool(argstr='%d', position=6,
                                     requires=['radian_fraction'],
                                     desc='whether the rotation is searched'
                                          ' around an initial principal axis'
                                          ' alignment')
    use_local_search = traits.Int(argstr='%d', position=7,
                                  requires=['use_principal_axes'],
                                  desc='the number of local optimization'
                                       ' iterations is run at each search'
                                       ' point')
    image_mask = File(argstr='%s', exists=True, position=8,
                      requires=['use_local_search'],
                      desc='mask constraining the initializer')


class AffineInitializerOutputSpec(TraitedSpec):
    affine_transform = File(exists=True, desc='output transform file')


class AffineInitializer(ANTSCommand):
    """
    Examples
    --------
    
    >>> initalizer = AffineInitializer()
    >>> initalizer.inputs.dimension = 3
    >>> initalizer.inputs.fixed_image = 'NIFTI/volume003.nii.gz'
    >>> initalizer.inputs.moving_image = 'NIFTI/volume004.nii.gz'
    >>> initalizer.inputs.output_affine_transform = 'initial_xform.mat'
    >>> initalizer.inputs.search_factor = 10
    >>> initalizer.inputs.radian_fraction = 0.3
    >>> initalizer.inputs.use_principal_axes = True
    >>> initalizer.inputs.use_local_search = 20
    >>> initalizer.inputs.image_mask = 'mask/mask.nii.gz'
    >>> initalizer.cmdline
    'antsAffineInitializer 3 NIFTI/volume003.nii.gz NIFTI/volume004.nii.gz initial_xform.mat 10 0.3 1 20 mask/mask.nii.gz'
    """
    _cmd = 'antsAffineInitializer'
    input_spec = AffineInitializerInputSpec
    output_spec = AffineInitializerOutputSpec
    
    
    def _format_arg(self, opt, spec, val):
        if opt == 'radian_fraction':
            # Suppress extraneous trailing zeros.
            # Cf. http://stackoverflow.com/questions/2440692/formatting-floats-in-python-without-superfluous-zeros
            return ('%f' % val).rstrip('0').rstrip('.')
        else:
            return super(AffineInitializer, self)._format_arg(opt, spec, val)
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        out_file = os.path.abspath(self.inputs.output_affine_transform)
        outputs['affine_transform'] = out_file
        return outputs
