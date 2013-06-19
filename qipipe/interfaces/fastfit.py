"""
This module wraps the proprietary OHSU AIRC ``fastfit`` software.
``fastfit`` optimizes the input pharmacokinetic model.
"""
import os
from os import path
from glob import glob
import traits.api as traits
from nipype.interfaces.base import (DynamicTraitedSpec,
                                    CommandLine, 
                                    CommandLineInputSpec)
from nipype.interfaces.traits_extension import Undefined

class MpiCommandLineInputSpec(CommandLineInputSpec):
    use_mpi = traits.Bool(False, 
                          desc='Whether or not to run the command with mpiexec',
                          usedefault=True)
    n_procs = traits.Int(desc='Num processors to specify to mpiexec')
    

class MpiCommandLine(CommandLine):
    @property
    def cmdline(self):
        """Adds 'mpiexec' to begining of command"""
        result = []
        if self.inputs.use_mpi:
            result.append('mpiexec')
            if self.inputs.n_procs: 
                result.append('-n %d' % self.inputs.n_procs)
        result.append(super(MpiCommandLine, self).cmdline)
        return ' '.join(result)
        

class FastfitInputSpec(MpiCommandLineInputSpec):
    model_name = traits.String(desc='The name of the model to optimize',
                               mandatory=True,
                               position=-2,
                               argstr='%s'
                              )
    target_data = traits.File(desc='Target data', 
                              mandatory=True, 
                              position=-1, 
                              argstr='%s'
                             )
    mask = traits.File(desc='Mask file', argstr='-m %s')
    weights = traits.File(desc='Weights file', argstr='-w %s')
    params = traits.Dict(desc='Parameters for the model')
    params_csv = traits.File(desc='Parameters CSV', 
                             argstr='--param-csv %s')
    optional_outs = traits.List(desc='Optional outputs to produce',
                                argstr='%s')

class Fastfit(MpiCommandLine):
    _cmd = 'fastfit'
    input_spec = FastfitInputSpec
    output_spec = DynamicTraitedSpec
    
    def _format_arg(self, name, spec, value):
        if name == 'optional_outs':
            return ' '.join("-o '%s'" % opt_out for opt_out in value)
        elif name == 'params':
            return ' '.join(["-s %s:%s" % item 
                             for item in value.iteritems()])
        else:
            return spec.argstr % value
            
    def _outputs(self):
        #Set up dynamic outputs for resulting parameter maps
        full_module_name = 'fastfit.models.%s' % self.inputs.model_name
        model_module = __import__(full_module_name, [], [], ['fastfit.models'])
        self._opt_params = model_module.model.optimization_params
        outputs = super(Fastfit, self)._outputs()
        undefined_traits = {}
        for param_name in self._opt_params:
            outputs.add_trait(param_name, traits.File(exists=True))
            undefined_traits[param_name] = Undefined
        
        #Set up dynamic outputs for any requested optional outputs
        if self.inputs.optional_outs:
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
            outputs[param_name] = path.join(cwd, 
                                            '%s_map.nii.gz' % param_name)
        
        return outputs
