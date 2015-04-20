import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import CopyMeta
from nipype.interfaces.utility import (IdentityInterface, Function, Merge)
import qiutil
from qiutil.logging import logger
from .. import project
from ..interfaces import XNATUpload
from ..helpers.bolus_arrival import bolus_arrival_index, BolusArrivalError
from .workflow_base import WorkflowBase
from .distributable import DISTRIBUTABLE

PK_PREFIX = 'pk'
"""The XNAT modeling resource object label prefix."""


def run(subject, session, scan, time_series, **opts):
    """
    Creates a :class:`qipipe.pipeline.modeling.ModelingWorkflow` and
    runs it on the given inputs.

    :param subject: the input subject
    :param session: the input session
    :param scan: input scan
    :param time_series: the input 4D NiFTI time series
    :param opts: the :class:`qipipe.pipeline.modeling.ModelingWorkflow`
        initializer options
    :return: the :meth:`qipipe.pipeline.modeling.ModelingWorkflow.run`
        result
    """
    mask = opts.pop('mask', None)
    wf = ModelingWorkflow(**opts)
    
    return wf.run(subject, session, scan, time_series, mask=mask)


class ModelingWorkflow(WorkflowBase):

    """
    The ModelingWorkflow builds and executes the Nipype pharmacokinetic
    mapping workflow.

    The workflow calculates the modeling parameters for an input 4D
    time series NiFTI image file as follows:

    - Compute the |R10| value, if it is not given in the options

    - Convert the DCE time series to a R1 map series

    - Determine the AIF and R1 fit parameters from the time series

    - Optimize the BOLERO pharmacokinetic model

    - Upload the modeling result to XNAT

    The modeling workflow input is the `input_spec` node consisting of the
    following input fields:

    - *subject*: the subject name

    - *session*: the session name

    - *mask*: the mask to apply to the images

    - *time_series*: the 4D time series NiFTI file to model

    - *bolus_arrival_index*: the bolus uptake volume index

    - the PK modeling parameters described below

    If an input field is defined in the configuration file ``Parameters``
    topic, then the input field is set to that value.

    If the |R10| option is not set, then it is computed from the proton
    density weighted scans and DCE series baseline image.

    The outputs are collected in the `output_spec` node for the FXL
    (`Tofts standard`_) model and the FXR (`shutter speed`_) model with
    the following fields:

    - `r1_series`: the R1 series files

    - `pk_params`: the AIF and R1 parameter CSV file

    - `fxr_k_trans`, `fxl_k_trans`: the |Ktrans| vascular permeability
       transfer constant

    - `delta_k_trans`: the FXR-FXL |Ktrans| difference

    - `fxr_v_e`, `fxl_v_e`: the |ve| extravascular extracellular volume
       fraction

    - `fxr_tau_i`: the |taui| intracellular |H2O| mean lifetime

    - `fxr_chi_sq`, `fxl_chi_sq`: the |chisq| intensity goodness of fit

    In addition, if |R10| is computed, then the output includes the
    following fields:

    - `pdw_image`: the proton density weighted image

    - `dce_baseline`: the DCE series baseline image

    - `r1_0`: the computed |R10| value

    This workflow is adapted from the `AIRC DCE`_ implementation.

    :Note: This workflow uses proprietary OHSU AIRC software, notably the
        BOLERO implementation of the shutter speed model.

    .. reST substitutions:
    .. include:: <isogrk3.txt>
    .. |H2O| replace:: H\ :sub:`2`\ O
    .. |Ktrans| replace:: K\ :sup:`trans`
    .. |ve| replace:: v\ :sub:`e`
    .. |taui| replace:: |tau|\ :sub:`i`
    .. |chisq| replace:: |chi|\ :sup:`2`
    .. |R10| replace:: R1\ :sub:`0`

    .. _Tofts standard: http://onlinelibrary.wiley.com/doi/10.1002/(SICI)1522-2586(199909)10:3%3C223::AID-JMRI2%3E3.0.CO;2-S/abstract
    .. _shutter speed: http://www.ncbi.nlm.nih.gov/pmc/articles/PMC2582583
    .. _AIRC DCE: https://everett.ohsu.edu/hg/qin_dce
    """

    def __init__(self, **opts):
        """
        The modeling parameters can be defined in either the options or the
        configuration as follows:

        - The parameters can be defined in the configuration
          ``Parameters`` section.

        - The input options take precedence over the configuration
          settings.

        - The *r1_0_val* takes precedence over the R1_0 computation
          fields *pd_dir* and *max_r1_0*. If *r1_0_val* is set
          in the input options, then *pd_dir* and *max_r1_0* are
          not included from the result.

        - If *pd_dir* and *max_r1_0* are set in the input options
          and *r1_0_val* is not set in the input options, then
          a *r1_0_val* configuration setting is ignored.

        - The *baseline_end_idx* defaults to 1 if it is not set in
          either the input options or the configuration.

        :param opts: the following initialization options:
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword r1_0_val: the optional fixed |R10| value
        :keyword max_r1_0: the maximum computed |R10| value, if the fixed
            |R10| option is not set
        :keyword pd_dir: the proton density files parent directory, if the
            fixed |R10| option is not set
        :keyword baseline_end_idx: the number of volumes to merge into a R1
            series baseline image (default is 1)
        """
        cfg_file = opts.pop('cfg_file', None)
        super(ModelingWorkflow, self).__init__(logger(__name__), cfg_file)

        resource = opts.pop('modeling', None)
        if not resource:
            resource = self._generate_resource_name()
        self.resource = resource
        """
        The XNAT resource name for all executions of this
        :class:`qipipe.pipeline.modeling.ModelingWorkflow` instance. The
        name is unique, which permits more than one model to be stored for
        each input volume without a name conflict.
        """

        self.workflow = self._create_workflow(**opts)
        """
        The modeling workflow described in
        :class:`qipipe.pipeline.modeling.ModelingWorkflow`.
        """

    def run(self, subject, session, scan, time_series, mask=None):
        """
        Executes the modeling workflow described in
        :class:`qipipe.pipeline.modeling.ModelingWorkflow`
        on the given input time series resource. The time series can
        be the merged scan NIFTI files or merged registration files.

        This run method connects the given inputs to the modeling workflow
        inputs. The execution workflow is then executed, resulting in a
        new uploaded XNAT resource.

        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param time_series: the 4D time series resource name
        :param mask: the XNAT mask resource
        :return: the modeling result XNAT resource name
        """
        self._logger.debug("Modeling the %s %s %s time series..." %
            (sbj, sess, time_series))

        # Determine the bolus uptake. If it could not be determined,
        # then take the first volume as the uptake.
        try:
            bolus_arrival_index = bolus_arrival_index(time_series)
        except BolusArrivalError:
            bolus_arrival_index = 0

        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.time_series = time_series
        input_spec.inputs.bolus_arrival_index = bolus_arrival_index
        input_spec.inputs.mask = mask

        # Execute the modeling workflow.
        self._logger.debug("Executing the %s workflow on the %s %s scan %d"
                           " %s time series..." %
                           (self.workflow.name, subject, session, scan, time_series))
        self._run_workflow(self.workflow)
        self._logger.debug("Executed the %s workflow on the %s %s scan %d %s"
                            " time series." %
                           (self.workflow.name, subject, session, scan, time_series))
        
        # Return the resource name.
        return self.resource

    def _generate_resource_name(self):
        """
        Makes a unique modeling resource name. Uniqueness permits more than
        one resource to be stored for a given session without a name conflict.

        :return: a unique XNAT modeling resource name
        """
        return '_'.join((PK_PREFIX, qiutil.file.generate_file_name()))

    def _create_workflow(self, base_dir=None, **opts):
        """
        Builds the modeling workflow.

        :param base_dir: the execution working directory
            (default is a new temp directory)
        :param opts: the additional workflow initialization parameters
        :return: the Nipype workflow
        """
        self._logger.debug("Building the modeling workflow...")

        # The base workflow.
        if not base_dir:
            base_dir = tempfile.mkdtemp()
        mdl_wf = pe.Workflow(name='modeling', base_dir=base_dir)

        # Start with a base workflow.
        base_wf = self._create_base_workflow(base_dir=base_dir, **opts)

        # The workflow input fields.
        in_fields = ['subject', 'session', 'scan', 'time_series', 'mask',
                     'bolus_arrival_index']
        input_xfc = IdentityInterface(fields=in_fields)
        input_spec = pe.Node(input_xfc, name='input_spec')
        self._logger.debug("The modeling workflow input is %s with"
            " fields %s" % (input_spec.name, in_fields))
        mdl_wf.connect(input_spec, 'time_series',
                       base_wf, 'input_spec.time_series')
        mdl_wf.connect(input_spec, 'mask', base_wf, 'input_spec.mask')
        mdl_wf.connect(input_spec, 'bolus_arrival_index',
                       base_wf, 'input_spec.bolus_arrival_index')

        # Upload the modeling results to XNAT.
        # Each output field contains a modeling result file.
        # Upload these files to the modeling resource.
        base_output = base_wf.get_node('output_spec')
        out_fields = base_output.outputs.copyable_trait_names()
        merge_outputs = pe.Node(Merge(len(out_fields)), name='merge_outputs')
        for i, field in enumerate(out_fields):
            base_field = 'output_spec.' + field
            mdl_wf.connect(base_wf, base_field, merge_outputs, "in%d" % (i + 1))
        upload_xfc = XNATUpload(project=project(), resource=self.resource)
        upload_node = pe.Node(upload_xfc, name='upload_outputs')
        mdl_wf.connect(input_spec, 'subject', upload_node, 'subject')
        mdl_wf.connect(input_spec, 'session', upload_node, 'session')
        mdl_wf.connect(input_spec, 'scan', upload_node, 'scan')
        mdl_wf.connect(merge_outputs, 'out', upload_node, 'in_files')

        # TODO - Get the overall and ROI FSL mean intensity values.

        # The output is the modeling outputs.
        output_xfc = IdentityInterface(fields=out_fields)
        output_spec = pe.Node(output_xfc, name='output_spec')
        for field in out_fields:
            base_field = 'output_spec.' + field
            mdl_wf.connect(base_wf, base_field, output_spec, field)
        self._logger.debug("The modeling workflow output is %s with"
                           " fields %s" % (output_spec.name, out_fields))

        self._configure_nodes(mdl_wf)

        self._logger.debug("Created the %s workflow." % mdl_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self.depict_workflow(mdl_wf)

        return mdl_wf

    def _create_base_workflow(self, base_dir=None, **opts):
        """
        Creates the modeling base workflow. This workflow performs the steps
        described in :class:`qipipe.pipeline.modeling.ModelingWorkflow` with
        the exception of XNAT upload.

        :Note: This workflow is adapted from the AIRC workflow at
        https://everett.ohsu.edu/hg/qin_dce. The AIRC workflow time series
        merge is removed and added as input to the workflow created by this
        method. Any change to the ``qin_dce`` workflow should be reflected in
        this method.

        :param base_dir: the workflow working directory
        :param opts: the PK modeling parameters
        :return: the pyxnat Workflow
        """
        base_wf = pe.Workflow(name='modeling_base', base_dir=base_dir)

        # The PK modeling parameters.
        opts = self._pk_parameters(**opts)
        # Set the use_fixed_r1_0 flag.
        use_fixed_r1_0 = not not opts.get('r1_0_val')

        # Set up the input node.
        non_pk_flds = ['time_series', 'mask', 'bolus_arrival_index']
        in_fields = non_pk_flds + opts.keys()
        input_xfc = IdentityInterface(fields=in_fields, **opts)
        input_spec = pe.Node(input_xfc, name='input_spec')
        # Set the config parameters.
        for field in in_fields:
            if field in opts:
                setattr(input_spec.inputs, field, opts[field])

        # If we are not using a fixed r1_0 value, then compute a map from a
        # proton density weighted scan and the baseline of the DCE series.
        if not use_fixed_r1_0:
            # Create the DCE baseline image.
            make_base_func = Function(
                input_names=['time_series', 'baseline_end_idx'],
                output_names=['baseline'],
                function=make_baseline),
            make_base = pe.Node(make_base_func, name='make_base')
            base_wf.connect(input_spec, 'time_series',
                            make_base, 'time_series')
            base_wf.connect(input_spec, 'baseline_end_idx',
                            make_base, 'baseline_end_idx')

            # Create the R1_0 map.
            get_r1_0_func = Function(
                input_names=['pdw_image', 't1w_image', 'max_r1_0',
                             'mask_file'],
                output_names=['r1_0_map'],
                function=make_r1_0),
            get_r1_0 = pe.Node(get_r1_0_func, name='get_r1_0')
            base_wf.connect(input_spec, 'pd_nii', get_r1_0, 'pdw_image')
            base_wf.connect(make_base, 'baseline', get_r1_0, 't1w_image')
            base_wf.connect(input_spec, 'max_r1_0', get_r1_0, 'max_r1_0')
            base_wf.connect(input_spec, 'mask', get_r1_0, 'mask_file')

        # Convert the DCE time series to R1 maps.
        get_r1_series_func = Function(
            input_names=['time_series', 'r1_0', 'baseline_end', 'mask_file'],
            output_names=['r1_series'], function=make_r1_series)
        get_r1_series = pe.Node(get_r1_series_func, name='get_r1_series')
        base_wf.connect(input_spec, 'time_series',
                        get_r1_series, 'time_series')
        base_wf.connect(input_spec, 'baseline_end_idx',
                        get_r1_series, 'baseline_end')
        base_wf.connect(input_spec, 'mask', get_r1_series, 'mask_file')
        if use_fixed_r1_0:
            base_wf.connect(input_spec, 'r1_0_val', get_r1_series, 'r1_0')
        else:
            base_wf.connect(get_r1_0, 'r1_0_map', get_r1_series, 'r1_0')

        # Copy the time series meta-data to the R1 series.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        copy_meta.inputs.include_classes = [('global', 'const'),
                                            ('time', 'samples')]
        base_wf.connect(input_spec, 'time_series', copy_meta, 'src_file')
        base_wf.connect(get_r1_series, 'r1_series', copy_meta, 'dest_file')

        # Get the pharmacokinetic mapping parameters.
        get_params_flds = ['time_series', 'bolus_arrival_index']
        get_params_func = Function(input_names=get_params_flds,
                                   output_names=['params_csv'],
                                   function=get_fit_params)
        get_params = pe.Node(get_params_func, name='get_params')
        base_wf.connect(input_spec, 'time_series', get_params, 'time_series')
        base_wf.connect(input_spec, 'bolus_arrival_index',
                        get_params, 'bolus_arrival_index')

        # Import Fastfit on demand. This allows the modeling module to be
        # imported by clients without the proprietary Fastfit modeling
        # tool.
        try:
            Fastfit
        except NameError:
            from ..interfaces.fastfit import Fastfit
        # Optimize the pharmacokinetic model.
        pk_map = pe.Node(Fastfit(), name='pk_map')
        pk_map.inputs.model_name = 'fxr.model'
        pk_map.inputs.optional_outs = ['chisq', 'guess_model.k_trans',
                                       'guess_model.v_e', 'guess_model.chisq']
        base_wf.connect(copy_meta, 'dest_file', pk_map, 'target_data')
        base_wf.connect(input_spec, 'mask', pk_map, 'mask')
        base_wf.connect(get_params, 'params_csv', pk_map, 'params_csv')
        # Set the MPI flag.
        pk_map.inputs.use_mpi = DISTRIBUTABLE

        # Compute the Ktrans difference.
        delta_k_trans = pe.Node(fsl.ImageMaths(), name='delta_k_trans')
        delta_k_trans.inputs.op_string = '-sub'
        base_wf.connect(pk_map, 'k_trans', delta_k_trans, 'in_file')
        base_wf.connect(pk_map, 'guess_model.k_trans',
                        delta_k_trans, 'in_file2')

        # Set up the outputs.
        outputs = ['r1_series', 'pk_params', 'fxr_k_trans', 'fxr_v_e',
                   'fxr_tau_i', 'fxr_chisq', 'fxl_k_trans', 'fxl_v_e',
                   'fxl_chisq', 'delta_k_trans']
        if not use_fixed_r1_0:
            outputs += ['dce_baseline', 'r1_0']
        output_spec = pe.Node(IdentityInterface(fields=outputs),
                              name='output_spec')
        base_wf.connect(copy_meta, 'dest_file', output_spec, 'r1_series')
        base_wf.connect(get_params, 'params_csv', output_spec, 'pk_params')
        base_wf.connect(pk_map, 'k_trans', output_spec, 'fxr_k_trans')
        base_wf.connect(pk_map, 'v_e', output_spec, 'fxr_v_e')
        base_wf.connect(pk_map, 'tau_i', output_spec, 'fxr_tau_i')
        base_wf.connect(pk_map, 'chisq', output_spec, 'fxr_chisq')
        base_wf.connect(pk_map, 'guess_model.k_trans',
                        output_spec, 'fxl_k_trans')
        base_wf.connect(pk_map, 'guess_model.v_e', output_spec, 'fxl_v_e')
        base_wf.connect(pk_map, 'guess_model.chisq', output_spec, 'fxl_chisq')
        base_wf.connect(delta_k_trans, 'out_file',
                        output_spec, 'delta_k_trans')

        if not use_fixed_r1_0:
            base_wf.connect(make_base, 'baseline',
                            output_spec, 'dce_baseline')
            base_wf.connect(get_r1_0, 'r1_0_map', output_spec, 'r1_0')

        self._configure_nodes(base_wf)

        return base_wf

    def _pk_parameters(self, **opts):
        """
        Collects the modeling parameters defined in either the options
        or the configuration, as described in :class:`ModelingWorkflow`.

        :param opts: the input options
        :return: the parameter {name: value} dictionary
        """
        config = self.configuration.get('Parameters', {})

        logger(__name__).debug("Setting the PK parameters from the option"
            " keyword parameters %s and configuration %s..." % (opts, config))

        # The R1_0 computation fields.
        r1_fields = ['pd_dir', 'max_r1_0']
        # All of the possible fields.
        fields = set(r1_fields).update(['baseline_end_idx', 'r1_0_val'])
        # Validate the input options.
        for field in opts.iterkeys():
            if field not in fields:
                raise KeyError("The PK paramter is not recognized: %s" % field)

        if 'baseline_end_idx' not in opts:
            # Look for the the baseline parameter in the configuration.
            if 'baseline_end_idx' in config:
                opts['baseline_end_idx'] = config['baseline_end_idx']
            else:
                # The default baseline image count.
                opts['baseline_end_idx'] = 1

        # Set the use_fixed_r1_0 variable to None, signifying unknown.
        use_fixed_r1_0 = None
        # Get the R1_0 parameter values.
        if 'r1_0_val' in opts:
            r1_0_val = opts.get('r1_0_val')
            if r1_0_val:
                use_fixed_r1_0 = True
            else:
                use_fixed_r1_0 = False
        else:
            for field in r1_fields:
                value = opts.get(field)
                if value:
                    use_fixed_r1_0 = False

        # If none of the R1_0 options are set in the options,
        # then try the configuration.
        if use_fixed_r1_0 == None:
            r1_0_val = config.get('r1_0_val')
            if r1_0_val:
                opts['r1_0_val'] = r1_0_val
                use_fixed_r1_0 = True

        # If R1_0 is not fixed, then augment the R1_0 options
        # from the configuration, if necessary.
        if not use_fixed_r1_0:
            for field in r1_fields:
                if field not in opts and field in config:
                    use_fixed_r1_0 = False
                    opts[field] = config[field]
                # Validate the R1 parameter.
                if not opts.get(field):
                    raise ValueError("Missing both the r1_0_val and the %s"
                        " parameter." % field)

        # If the use_fixed_r1_0 flag is set, then remove the
        # extraneous R1 computation fields.
        if use_fixed_r1_0:
            for field in r1_fields:
                opts.pop(field, None)

        self._logger.debug("The PK modeling parameters: %s" % opts)
        return opts


### Utility functions called by workflow nodes. ###

def make_baseline(time_series, baseline_end_idx):
    """
    Makes the R1_0 computation baseline NiFTI file.

    :param time_series: the modeling input 4D NiFTI image
    :param baseline_end_idx: the exclusive limit of the baseline
        computation input series
    :return: the baseline NiFTI file name
    :raise ValueError: if the end index is a negative number
    """
    from dcmstack.dcmmeta import NiftiWrapper

    if baseline_end_idx <= 0:
        raise ValueError("The R1_0 computation baseline end index input value"
                         " is not a positive number: %s" % baseline_end_idx)
    nii = nb.load(time_series)
    nw = NiftiWrapper(nii)

    baselines = []
    for idx, split_nii in nw.split():
        if idx == baseline_end_idx:
            break
        baselines.append(split_nii)

    if len(baselines) == 1:
        return baselines[0]
    else:
        baseline = NiftiWrapper.from_sequence(baselines)
    baseline_path = path.join(os.getcwd(), 'baseline.nii.gz')
    nb.save(baseline, baseline_path)

    return baseline_path


def make_r1_0(pdw_image, t1w_image, max_r1_0, mask_file, **kwargs):
    """
    Returns the R1_0 map NiFTI file from the given proton density
    and T1-weighted images. The R1_0 map is computed using the
    ``pdw_t1w_to_r1`` function. The ``pdw_t1w_to_r1`` module
    must be in the Python path.

    :param pdw_image: the proton density NiFTI image file path
    :param t1w_image: the T1-weighted image file path
    :param max_r1_0: the R1_0 range maximum
    :param mask_file: the modeling mask file path
    :param kwargs: the ``pdw_t1w_to_r1`` function keyword arguments
    :return: the R1_0 map NiFTI image file name
    """
    import os
    import nibabel as nb
    import numpy as np
    from pdw_t1w_to_r1 import pdw_t1w_to_r1
    from dcmstack.dcmmeta import NiftiWrapper

    pdw_nw = NiftiWrapper(nb.load(pdw_image), make_empty=True)
    t1w_nw = NiftiWrapper(nb.load(t1w_image), make_empty=True)
    r1_space = np.arange(0.01, max_r1_0, 0.01)
    mask = nb.load(mask_file).get_data()
    r1_0 = pdw_t1w_to_r1(pdw_nw, t1w_nw, r1_space=r1_space, mask=mask,
                         **kwargs)

    cwd = os.getcwd()
    out_nii = nb.Nifti1Image(r1_0, pdw_nw.nii_img.get_affine())
    out_fn = os.path.join(cwd, 'r1_0_map.nii.gz')
    nb.save(out_nii, out_fn)

    return out_fn


def make_r1_series(time_series, r1_0, mask_file, **kwargs):
    """
    Makes the R1_0 series NiFTI file.

    :param time_series: the modeling input 4D NiFTI image
    :param r1_0: the R1_0 image file path
    :param mask_file: the modeling mask file path
    :return: the R1_0 series NiFTI image file name
    """
    import os
    import nibabel as nb
    from dce_to_r1 import dce_to_r1
    from dcmstack.dcmmeta import NiftiWrapper

    dce_nw = NiftiWrapper(nb.load(time_series), make_empty=True)
    if not isinstance(r1_0, float):
        r1_0 = nb.load(r1_0).get_data()
    mask = nb.load(mask_file).get_data()
    r1_series = dce_to_r1(dce_nw, r1_0, mask=mask, **kwargs)

    cwd = os.getcwd()
    out_nii = nb.Nifti1Image(r1_series, dce_nw.nii_img.get_affine())
    out_fn = os.path.join(cwd, 'r1_series.nii.gz')
    nb.save(out_nii, out_fn)

    return out_fn


def get_fit_params(time_series, bolus_arrival_index):
    """
    Obtains the following modeling fit parameters:

    * *aif_params*: arterial input function parameter array
    * *aif_delta_t*: acquisition time deltas
    * *aif_shift*: acquisition time shift
    * *r1_cr*: contrast R1
    * *r1_b_pre*: pre-contrast R1

    :param time_series: the modeling input 4D NiFTI image
    :param bolus_arrival_index: the bolus uptake series index
    :return: the parameter CSV file path
    """
    import os
    import csv
    import nibabel as nb
    import numpy as np
    from dcmstack.dcmmeta import NiftiWrapper
    from dcmstack import dcm_time_to_sec

    # Load the time series into a nifty NiFTI wrapper.
    nii = nb.load(time_series)
    nw = NiftiWrapper(nii)

    # The AIF shift parameter is the offset of the bolus arrival
    # series mid-point acquisition time from the MR session start
    # time.
    acq_time0 = dcm_time_to_sec(nw.get_meta('AcquisitionTime', (0, 0, 0, 0)))
    acq_time1 = dcm_time_to_sec(
        nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_arrival_index)))
    acq_time2 = dcm_time_to_sec(
        nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_arrival_index + 1)))
    aif_shift = ((acq_time1 + acq_time2) / 2.0) - acq_time0

    # Create the parameter CSV file.
    with open('params.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['aif_params', '0.674', '0.4', '2.2',
                             '0.23', '1.3', '0.09', '0.0013', '0.0'])
        csv_writer.writerow(['aif_delta_t', '1.5'])
        csv_writer.writerow(['aif_shift', str(aif_shift)])
        csv_writer.writerow(['r1_cr', '3.8'])
        csv_writer.writerow(['r1_b_pre', '0.71'])

    return os.path.join(os.getcwd(), 'params.csv')
