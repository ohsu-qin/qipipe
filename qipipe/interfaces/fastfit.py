"""
OHSU - This module wraps the proprietary OHSU AIRC ``fastfit``
software. ``fastfit`` optimizes the input pharmacokinetic model.

:Note: this interface is copied from the AIRC cluster file
    ``/usr/global/scripts/fastfit_iface.py``. It is included
    in :mod:`qipipe.interfaces` in order to gather all custom OHSU
    QIN Python source code in the ``qipipe`` module. If it were not
    included, then qipipe would not compile in a non-AIRC cluster
    environment, even if this ``FastFit`` interface is unused.
"""

# TODO - move this to the ohsu-qin.pipeline.interfaces module

# Absolute import (the default in a future Python release) resolves
# the fastfit import as the installed fastfit Python module rather
# than this interface of the same name.
from __future__ import absolute_import
import os
from os import path
from glob import glob
import traits.api as traits
from nipype.interfaces.base import (
    DynamicTraitedSpec, MpiCommandLine, MpiCommandLineInputSpec,
    isdefined
)
from nipype.interfaces.traits_extension import Undefined
# Tolerate import even when the proprietary OHSU fastfit modeling
# tool is not available. This allows import of the modeling module
# by clients without fastfit.
try:
    from fastfit_iface import get_available_models
except:
    import warnings
    warnings.warn("The Fastfit modeling interface is not supported.")
from .interface_error import InterfaceError


class FastfitInputSpec(MpiCommandLineInputSpec):
    model_name = traits.String(desc='The name of the model to optimize',
                               mandatory=True, position=-2, argstr='%s')
    
    target_data = traits.File(desc='Target data',
                              mandatory=True, position=-1, argstr='%s')
    
    mask = traits.File(desc='Mask file', argstr='-m %s')
    
    weights = traits.File(desc='Weights file', argstr='-w %s')
    
    params = traits.Dict(desc='Parameters for the model')
    
    params_csv = traits.File(desc='Parameters CSV', argstr='--param-csv %s')
    
    fix_params = traits.Dict(desc="Optimization parameters to fix, and "
                             "the values to fix them to", argstr='%s')
    
    optional_outs = traits.List(desc='Optional outputs to produce',
                                argstr='%s')


class Fastfit(MpiCommandLine):
    """
    Interface to the ``fastfit`` software package.
    
    Input parameters:
    
    * *min_outs*: The minimum outputs expected. Required if the *model_name*
      input is set dynamically.
    """
    
    # Note: a local fastfit build results in MPICOMM errors.
    # The work-around is to specify a fastfit wrapper that loads a
    # virtual environment before calling the global fastfit.
    _cmd = 'global_fastfit'
    
    input_spec = FastfitInputSpec
    
    output_spec = DynamicTraitedSpec
    
    def __init__(self, min_outs=None, **inputs):
        super(Fastfit, self).__init__(**inputs)
        self._min_outs = min_outs

    # Note: nipype MpiCommandLine calls mpiexec, which resolves to the
    # local build mpiexec. This results in MPICOMM errors, as described
    # in the _cmd note above. The work-around is to replace mpiexec
    # with the absolute path of the official mpiexec.
    @property
    def cmdline(self):
        result = super(Fastfit, self).cmdline
        return result.replace('mpiexec', '/usr/global/bin/mpiexec')
    
    def _format_arg(self, name, spec, value):
        if name == 'optional_outs':
            return ' '.join("-o '%s'" % opt_out for opt_out in value)
        elif name == 'params':
            return ' '.join(["-s %s:%s" % item
                             for item in value.iteritems()])
        elif name == 'fix_params':
            return ' '.join(["--fix-param %s:%s" % item
                             for item in value.iteritems()])
        else:
            return spec.argstr % value
    
    def _outputs(self):
        #Set up dynamic outputs for resulting parameter maps
        outputs = super(Fastfit, self)._outputs()
        undefined_traits = {}
        
        #Get a list of fixed params
        fixed_params = []
        if isdefined(self.inputs.fix_params):
            for key in self.inputs.fix_params:
                fixed_params.append(key)
        
        #If the model name is static, we can find the outputs
        if isdefined(self.inputs.model_name):
            model_mods, builtin_models, user_models = get_available_models()
            model = model_mods[self.inputs.model_name]
            self._opt_params = model.optimization_params
            if (not self._min_outs is None and
                any(not out in self._opt_params
                    for out in self._min_outs)):
                raise InterfaceError("The model %s does not provide the "
                                 "minimum outputs" % model_name)
            for param_name in self._opt_params:
                if not param_name in fixed_params:
                    outputs.add_trait(param_name, traits.File(exists=True))
                    undefined_traits[param_name] = Undefined
        #Otherwise we need min_outs
        elif not self._min_outs is None:
            for param_name in self._min_outs:
                outputs.add_trait(param_name, traits.File(exists=True))
                undefined_traits[param_name] = Undefined
        else:
            raise InterfaceError("Either the 'model_name' input must "
                               "be static or the 'min_outs' argument "
                               "to the constructor must be given.")
        
        #Set up dynamic outputs for any requested optional outputs
        if isdefined(self.inputs.optional_outs):
            for opt_out in self.inputs.optional_outs:
                outputs.add_trait(opt_out, traits.File(exists=True))
                undefined_traits[opt_out] = Undefined
        
        outputs.trait_set(trait_change_notify=False, **undefined_traits)
        for dynamic_out in undefined_traits.keys():
            _ = getattr(outputs, dynamic_out) #Not sure why, but this is needed
        return outputs
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        
        cwd = os.getcwd()
        for param_name in outputs.keys():
            outputs[param_name] = path.join(cwd, '%s_map.nii.gz' % param_name)
        
        return outputs
