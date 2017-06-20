"""
OHSU - This module wraps the proprietary OHSU AIRC ``fastfit``
software. ``fastfit`` optimizes the input pharmacokinetic model.

.. Note:: this interface is adapted from the OHSU AIRC cluster file
    ``/usr/global/scripts/fastfit_iface.py``.
"""

# TODO - move this to the ohsu-qin.pipeline.interfaces module

# Note - as of Jun 2017, the old fastfit is broken and the new,
# improved fastfit is only available in a virtual environment.
# Therefore, it is necessary to override the global fastfit
# in the PATH with the following local fastfit script:
#
# #! env bash
#
# # Reset the path. If the system login path was captured in the
# # SYS_PATH envvar (preferred), then use that as the basis.
# # Otherwise, take our chances with the current PATH.
# if [ -n "$SYS_PATH" ]; then
#     PATH="/usr/global/venvs/fastfit/bin:$SYS_PATH"
# else
#     PATH="/usr/global/venvs/fastfit/bin:$PATH"
# fi
#
# # Borrowed from venv activate:
# # This should detect bash and zsh, which have a hash command
# # that must be called to get it to forget past commands.
# # Without forgetting past commands the $PATH changes we made
# # may not be respected.
# if [ -n "$BASH" -o -n "$ZSH_VERSION" ]; then
#     hash -r
# fi
#
# echo "Submitting `which mpiexec` `which fastfit` $*..."
# mpiexec fastfit $*
#
# TODO - Once /usr/global/bin/fastfit is upgraded, derive Fastfit
# from MpiCommand.
#

# Absolute import (the default in a future Python release) resolves
# the fastfit import as the installed fastfit Python module rather
# than this interface of the same name.
from __future__ import absolute_import
import os
from os import path
from glob import glob
from twisted.python.procutils import which
from nipype.interfaces.base import (
    traits, DynamicTraitedSpec, CommandLine, CommandLineInputSpec,
    isdefined
)
from nipype.interfaces.traits_extension import Undefined
from .interface_error import InterfaceError
from ..helpers.logging import logger


class FastfitInputSpec(CommandLineInputSpec):
    model_name = traits.String(desc='The name of the model to optimize',
                               mandatory=True, position=-2, argstr='%s')

    in_file = traits.File(
        desc='The source 4D NIfTI file containing the target data',
        mandatory=True, position=-1, argstr='%s'
    )

    mask = traits.File(desc='Mask file', argstr='-m %s')

    weights = traits.File(desc='Weights file', argstr='-w %s')

    optimization_params = traits.List(
        desc='The required optimization parameters for the model',
        mandatory=True
    )

    other_params = traits.Dict(desc='Other parameters for the model')

    other_params_csv = traits.File(desc='Other parameters CSV',
                                   argstr='--param-csv %s')

    fix_params = traits.Dict(desc="Optimization parameters to fix, and "
                             "the values to fix them to", argstr='%s')

    optional_outs = traits.List(desc='Optional outputs to produce',
                                argstr='%s')


class Fastfit(CommandLine):
    """
    Interface to the ``fastfit`` software package.
    """

    _cmd = 'fastfit'

    input_spec = FastfitInputSpec

    output_spec = DynamicTraitedSpec

    def __init__(self, **inputs):
        super(Fastfit, self).__init__(**inputs)

    # The cmdline override below is commented out, since it only
    # applies to the broken old fastfit. However, it may apply to
    # the new fastfit when it replaces the global fastfit.
    #
    # # Note: nipype MpiCommandLine calls mpiexec, which resolves
    # # to the Anaconda virtual environment mpiexec. This results
    # # in MPICOMM errors. The work-around is to replace mpiexec
    # # in the command with the absolute path of the global mpiexec.
    # @property
    # def cmdline(self):
    #     result = super(Fastfit, self).cmdline
    #     # Delay until this last possible moment the fastfit existence
    #     # check. This delay allows Fastfit interface creation without
    #     # the fastfit executable, e.g. in a dry run.
    #     matches = which('fastfit')
    #     if matches:
    #         logger(__name__).debug("The fastfit executable is %s" %
    #                                matches[0])
    #     else:
    #         raise InterfaceError("No fastfit executable was found in"
    #                              " the path %s" % os.environ('PATH'))
    #
    #     # MpiCommandLine wraps the command in a call to mpiexec.
    #     # In the OHSU AIRC context, the only mpiexec which can
    #     # run fastfit is in /usr/global/bin. Anaconda installs its
    #     # own mpiexec, which cannot be used because of a loadlib
    #     # incompatibility.
    #     return result.replace('mpiexec', '/usr/global/bin/mpiexec')

    def _format_arg(self, name, spec, value):
        if name == 'optional_outs':
            return ' '.join("-o '%s'" % opt_out for opt_out in value)
        elif name == 'other_params':
            return ' '.join(["--set-param %s:%s" % item
                             for item in value.iteritems()])
        elif name == 'fix_params':
            return ' '.join(["--fix-param %s:%s" % item
                             for item in value.iteritems()])
        else:
            return spec.argstr % value

    def _outputs(self):
        # Set up dynamic outputs for resulting parameter maps.
        outputs = super(Fastfit, self)._outputs()
        undefined_traits = {}

        # Get a list of fixed parameters.
        fixed_params = []
        if isdefined(self.inputs.fix_params):
            for key in self.inputs.fix_params:
                fixed_params.append(key)

        # Add optimization parameter outputs.
        self._opt_params = self.inputs.optimization_params
        for param_name in self._opt_params:
            if not param_name in fixed_params:
                outputs.add_trait(param_name, traits.File(exists=True))
                undefined_traits[param_name] = Undefined

        # Add optional outputs.
        if isdefined(self.inputs.optional_outs):
            for opt_out in self.inputs.optional_outs:
                outputs.add_trait(opt_out, traits.File(exists=True))
                undefined_traits[opt_out] = Undefined

        # Mystery code retained from fastfit_iface.py.
        outputs.trait_set(trait_change_notify=False, **undefined_traits)
        for dynamic_out in undefined_traits.keys():
            _ = getattr(outputs, dynamic_out)

        return outputs

    def _list_outputs(self):
        outputs = self._outputs().get()

        cwd = os.getcwd()
        for param_name in outputs.keys():
            outputs[param_name] = path.join(cwd, '%s_map.nii.gz' % param_name)

        return outputs
