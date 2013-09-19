"""
This module wraps the proprietary OHSU AIRC ``fastfit`` software.
``fastfit`` optimizes the input pharmacokinetic model.

:Note: this interface is copied from the AIRC cluster
    ``/usr/global/scripts/fastfit_iface.py`` file. It is included
    in :mod:`qipipe.interfaces` in order to gather all custom OHSU
    QIN Python source code in the ``qipipe`` module. If it were not
    included, then qipipe would not compile in a non-AIRC cluster
    environment, even if this ``FastFit`` interface is unused.
"""
import os
from os import path
import traits.api as traits
from nipype.interfaces.base import (DynamicTraitedSpec,
                                    CommandLine,
                                    CommandLineInputSpec,
                                    isdefined)
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
    fix_params = traits.Dict(desc="Optimization parameters to fix, and"
                             " the values to fix them to", argstr='%s')
    optional_outs = traits.List(desc='Optional outputs to produce',
                                argstr='%s')


class Fastfit(MpiCommandLine):

    """``Fastfit`` wraps the ``fastfit`` software package."""

    _cmd = 'fastfit'
    input_spec = FastfitInputSpec
    output_spec = DynamicTraitedSpec

    def __init__(self, min_outs=None, **inputs):
        """
        :param min_outs: the minimum outputs expected. Required if the
            *model_name* input is set dynamically.
        """
        super(Fastfit, self).__init__(**inputs)
        self._min_outs = min_outs

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
        """Sets up tje dynamic outputs for the resulting parameter maps."""
        outputs = super(Fastfit, self)._outputs()
        undefined_traits = {}

        # Get a list of fixed params
        fixed_params = []
        if isdefined(self.inputs.fix_params):
            for key in self.inputs.fix_params:
                fixed_params.append(key)

        # If the model name is static, we can find the outputs
        if isdefined(self.inputs.model_name):
            full_module_name = 'fastfit.models.%s' % self.inputs.model_name
            model_module = __import__(
                full_module_name, [], [], ['fastfit.models'])
            self._opt_params = model_module.model.optimization_params
            if (not self._min_outs is None and
                any(not out in self._opt_params
                    for out in self._min_outs)
                ):
                raise ValueError("The model %s does not provide the"
                                 " minimum outputs" % self.inputs.model_name)
            for param_name in self._opt_params:
                if not param_name in fixed_params:
                    outputs.add_trait(param_name, traits.File(exists=True))
                    undefined_traits[param_name] = Undefined

        # Otherwise we need min_outs
        elif not self._min_outs is None:
            for param_name in self._min_outs:
                outputs.add_trait(param_name, traits.File(exists=True))
                undefined_traits[param_name] = Undefined
        else:
            raise ValueError("Either the 'model_name' input must"
                             " be static or the 'min_outs' argument"
                             " to the constructor must be given.")

        # Set up dynamic outputs for any requested optional outputs
        if isdefined(self.inputs.optional_outs):
            for opt_out in self.inputs.optional_outs:
                outputs.add_trait(opt_out, traits.File(exists=True))
                undefined_traits[opt_out] = Undefined

        outputs.trait_set(trait_change_notify=False, **undefined_traits)
        for dynamic_out in undefined_traits.keys():
            # Not sure why, but this is needed
            _ = getattr(outputs, dynamic_out)
        return outputs

    def _list_outputs(self):
        outputs = self._outputs().get()

        cwd = os.getcwd()
        for param_name in outputs.keys():
            outputs[param_name] = path.join(cwd,
                                            '%s_map.nii.gz' % param_name)

        return outputs
