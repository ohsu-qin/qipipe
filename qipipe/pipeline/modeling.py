# This workflow is adapted from OHSU AIRC /usr/global/scripts/qin_dce.py
# with the following changes:
# * Several bugs were fixed, e.g. missing node imports and a missing
#   iteration enumerate call.
# * As of 5/24/17 the dce_prep utility function signatures were changed
#   and inconsistent with qin_dce.
# * The MriVolCluster has been pulled out into its own upstream node,
#   since the mask is also used for registration.
# * Functional changes, e.g. the ReadTheDocs conditional import, fit
#   the qipipe environment.
# * Stylistic changes, e.g. variable names and spacing, are more
#   consistent with qipipe.
#
import os
import logging
# The ReadTheDocs build does not include nipype.
# TODO - Reconfirm that RTD can't build with these.
on_rtd = os.environ.get('READTHEDOCS') == 'True'
if not on_rtd:
    from nipype.pipeline import engine as pe
    from nipype.interfaces import fsl
    from nipype.interfaces.dcmstack import CopyMeta
    from nipype.interfaces.utility import (IdentityInterface, Function, Merge)
import qiutil
from ..helpers.bolus_arrival import (bolus_arrival_index, BolusArrivalError)
from ..helpers.logging import logger
from ..helpers.constants import CONF_DIR
from ..interfaces import (
    DceToR1, Fastfit, StickyIdentityInterface, XNATUpload, XNATFind
)
from .workflow_base import WorkflowBase
from .pipeline_error import PipelineError

MODELING_PREFIX = 'pk_'
"""The modeling XNAT object label prefix."""

MODELING_CONF_FILE = 'modeling.cfg'
"""The modeling workflow configuration."""

OHSU_CONF_SECTIONS = ['Fastfit', 'R1', 'AIF']
"""The OHSU AIRC modeling configuration sections."""

FASTFIT_PARAMS_FILE = 'params.csv'
"""The Fastfit parameters CSV file name."""

FASTFIT_CONF_PROPS = ['model_name', 'optimization_params', 'optional_outs']
"""The Fastfit configuration property names."""

FXL_MODEL_PREFIX = 'ext_tofts.'
"""The Fastfit Standard TOFTS model prefix."""

class ModelingError(Exception):
    pass


def run(subject, session, scan, time_series, **opts):
    """
    Creates a :class:`qipipe.pipeline.modeling.ModelingWorkflow` and
    runs it on the given inputs.

    :param subject: the input subject
    :param session: the input session
    :param scan: input scan
    :param time_series: the input 4D NIfTI time series
    :param opts: the :class:`qipipe.pipeline.modeling.ModelingWorkflow`
        initializer and run options
    :return: the :meth:`qipipe.pipeline.modeling.ModelingWorkflow.run`
        result
    """
    run_opts = {key: opts.pop(key)
                for key in ['bolus_arrival_index', 'mask', 'registration']
                if key in opts}
    wf = ModelingWorkflow(**opts)

    return wf.run(subject, session, scan, time_series, **run_opts)


class ModelingWorkflow(WorkflowBase):
    """
    The ModelingWorkflow builds and executes the Nipype pharmacokinetic
    mapping workflow.

    The workflow calculates the modeling parameters for an input 4D
    time series NIfTI image file as follows:

    - Compute the |R10| value, if it is not given in the options

    - Convert the DCE time series to a R1 map series

    - Determine the AIF and R1 fit parameters from the time series

    - Optimize the OHSU pharmacokinetic model

    - Upload the modeling result to XNAT

    The modeling workflow input is the `input_spec` node consisting of the
    following input fields:

    - *subject*: the subject name

    - *session*: the session name

    - *mask*: the mask to apply to the images

    - *time_series*: the 4D time series NIfTI file to model

    - *bolus_arrival_index*: the bolus uptake volume index

    - the R1 modeling parameters described below

    If an input field is defined in the configuration file ``R1``
    section, then the input field is set to that value.

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

    - `pdw_file`: the proton density weighted image

    - `dce_baseline`: the DCE series baseline image

    - `r1_0`: the computed |R10| value

    This workflow is adapted from the `AIRC DCE`_ implementation.

    :Note: This workflow uses proprietary OHSU AIRC software, notably the
        OHSU implementation of the shutter speed model.

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

        - The parameters can be defined in the configuration ``R1``
          section.

        - The keyword arguments take precedence over the configuration
          settings.

        - The *r1_0_val* takes precedence over the R1_0 computation
          fields *pd_dir* and *max_r1_0*. If *r1_0_val* is set
          in the input options, then *pd_dir* and *max_r1_0* are
          not included from the result.

        - If *pd_dir* and *max_r1_0* are set in the input options
          and *r1_0_val* is not set in the input options, then
          a *r1_0_val* configuration setting is ignored.

        - The *base_end* defaults to 1 if it is not set in
          either the input options or the configuration.

        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer keyword arguments, as well as the following keyword arguments:
        :keyword r1_0_val: the optional fixed |R10| value
        :keyword max_r1_0: the maximum computed |R10| value, if the fixed
            |R10| option is not set
        :keyword pd_dir: the proton density files parent directory, if the
            fixed |R10| option is not set
        :keyword base_end: the number of volumes to merge into a R1
            series baseline image (default is 1)
        """
        super(ModelingWorkflow, self).__init__(__name__, **opts)

        technique_opt = opts.pop('technique', None)
        if not technique_opt:
            raise PipelineError('The modeling technique was not specified.')
        self.technique = technique_opt.lower()
        """The modeling technique. Built-in techniques include ``mock``."""

        self.resource = self._generate_resource_name()
        """
        The XNAT resource name for all executions of this
        :class:`qipipe.pipeline.modeling.ModelingWorkflow` instance.
        The name is unique, which permits more than one model to be
        stored for each input volume without a name conflict.
        """

        self.workflow = self._create_workflow(**opts)
        """
        The modeling workflow described in
        :class:`qipipe.pipeline.modeling.ModelingWorkflow`.
        """

    def run(self, subject, session, scan, time_series, **opts):
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
        :param time_series: the 4D modeling input time series file
            location
        :param opts: the following keyword parameters:
        :option bolus_arrival_index: the bolus uptake volume index
        :option mask: the XNAT mask resource name
        :return: the modeling result dictionary
        """
        self.logger.debug("Modeling the %s %s scan %d time series %s..." %
            (subject, session, scan, time_series))

        # Get the default bolus uptake, if necessary. If it cannot
        # be determined, then take the first volume as the uptake.
        bolus_arv_ndx = opts.pop('bolus_arrival_index', None)
        if bolus_arv_ndx == None:
            try:
                bolus_arv_ndx = bolus_arrival_index(time_series)
            except BolusArrivalError:
                bolus_arv_ndx = 0

        # The keyword parameters.
        mask = opts.get('mask')

        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.resource = self.resource
        input_spec.inputs.time_series = time_series
        input_spec.inputs.bolus_arrival_index = bolus_arv_ndx
        if mask:
            input_spec.inputs.mask = mask

        # Execute the modeling workflow.
        self.logger.debug(
            "Executing the %s workflow on the %s %s scan %d"
            " time series %s..." %
            (self.workflow.name, subject, session, scan, time_series)
        )
        wf_res = self._run_workflow(self.workflow)
        # If dry-run is set, then there is no result.
        if not wf_res:
            return {}
        # The magic incantation to get the Nipype workflow result.
        output_res = next(n for n in wf_res.nodes() if n.name == 'output_spec')
        results = output_res.inputs.get()['out_dict']

        self.logger.debug(
            "Executed the %s workflow on the %s %s scan %d"
            " time series %s with resource %s results:\n%s" %
            (self.workflow.name, subject, session, scan, time_series,
             self.resource, results)
        )

        # Return the modeling results.
        return results


    def _generate_resource_name(self):
        """
        Makes a unique modeling resource name. Uniqueness permits more than
        one resource to be stored for a given session without a name conflict.

        :return: a unique XNAT modeling resource name
        """
        return MODELING_PREFIX + qiutil.file.generate_file_name()

    def _create_workflow(self, **opts):
        """
        Builds the modeling workflow.

        :param opts: the additional workflow initialization parameters
        :return: the Nipype workflow
        """
        self.logger.debug("Building the modeling workflow...")

        # The supervisory workflow.
        mdl_wf = pe.Workflow(name='modeling', base_dir=self.base_dir)

        # The default modeling technique is the OHSU proprietary modeling
        # workflow.
        #
        # TODO - generalize workflow techniques here and in registration
        # to a module reference, e.g.:
        #
        # qipipe.cfg:
        # [Modeling]
        # technique = ohsu
        #
        # New git project with:
        # qipipe-ohsu/
        #   requirements.txt:
        #     qipipe==x.x.x
        #   modeling/ohsu.py:
        #     def create_workflow(**opts):
        #         ...
        #
        # pip install -e git+...
        #
        # Then replace below with:
        #
        # modules = technique_opt.split('.')
        # parent_opt = '.'.join(modules[:-1])
        # child_opt = modules[-1]
        # if parent_opt:
        #     workflow = __import__(parent_opt, globals(), locals(), [child_opt])
        # else:
        #     workflow = __import__(child_opt)
        # child_wf = workflow.create_workflow(**opts)
        #
        if self.technique == 'ohsu':
            child_wf = self._create_ohsu_workflow(**opts)
        elif self.technique == 'mock':
            child_wf = self._create_mock_workflow(**opts)
        elif self.technique:
            raise ModelingError("The modeling technique is unsupported:"
                                " %s" % self.technique)
        else:
            raise ModelingError('The modeling technique is missing')

        # The workflow input fields.
        in_fields = ['subject', 'session', 'scan', 'resource',
                     'time_series', 'mask', 'bolus_arrival_index']
        input_xfc = IdentityInterface(fields=in_fields)
        # The profile location is a temp file.
        input_spec = pe.Node(input_xfc, name='input_spec')
        self.logger.debug("The modeling workflow input is %s with"
            " fields %s" % (input_spec.name, in_fields))
        mdl_wf.connect(input_spec, 'time_series',
                       child_wf, 'input_spec.time_series')
        mdl_wf.connect(input_spec, 'mask', child_wf, 'input_spec.mask')
        mdl_wf.connect(input_spec, 'bolus_arrival_index',
                       child_wf, 'input_spec.bolus_arrival_index')

        # Make the profile.
        cr_prf_fields = ['technique', 'source', 'configuration', 'sections',
                         'dest']
        cr_prf_xfc = Function(input_names=cr_prf_fields,
                              output_names=['out_file'],
                              function=create_profile)
        cr_prf = pe.Node(cr_prf_xfc, name='create_profile')
        cr_prf.inputs.technique = self.technique
        cr_prf.inputs.configuration = self.configuration
        cr_prf.inputs.sections = self.profile_sections
        cr_prf.inputs.dest = MODELING_CONF_FILE
        mdl_wf.connect(input_spec, 'resource', cr_prf, 'source')

        # Each output field contains a modeling result file.
        child_output = child_wf.get_node('output_spec')
        out_fields = child_output.outputs.copyable_trait_names()
        # The files to upload include the profile and all
        # modeling result files.
        upload_file_cnt = len(out_fields) + 1
        concat_uploads = pe.Node(Merge(upload_file_cnt),
                                 name='concat_uploads')
        mdl_wf.connect(cr_prf, 'out_file', concat_uploads, 'in1')
        for i, field in enumerate(out_fields):
            child_field = 'output_spec.' + field
            mdl_wf.connect(child_wf, child_field,
                           concat_uploads, "in%d" % (i + 2))

        # Upload the profile and modeling results into the XNAT
        # modeling resource.
        upload_mdl_xfc = XNATUpload(project=self.project,
                                    resource=self.resource,
                                    modality='MR')
        upload_mdl = pe.Node(upload_mdl_xfc, name='upload_modeling')
        mdl_wf.connect(input_spec, 'subject', upload_mdl, 'subject')
        mdl_wf.connect(input_spec, 'session', upload_mdl, 'session')
        mdl_wf.connect(input_spec, 'scan', upload_mdl, 'scan')
        mdl_wf.connect(concat_uploads, 'out', upload_mdl, 'in_files')

        # Collect the outputs.
        merge_output_xfc = Merge(len(out_fields))
        merge_output = pe.Node(merge_output_xfc, name='merge_outputs')
        for i, field in enumerate(out_fields):
            child_field = 'output_spec.' + field
            merge_field = "in%d" % (i + 1)
            mdl_wf.connect(child_wf, child_field, merge_output, merge_field)
        assoc_xfc = Function(input_names=['names', 'values', 'target'],
                             output_names=['out_dict'],
                             function=associate)
        assoc_node = pe.Node(assoc_xfc, name='associate')
        assoc_node.inputs.names = out_fields
        mdl_wf.connect(merge_output, 'out', assoc_node, 'values')

        # The modeling workflow output node is a StickyIdentityInterface
        # rather than an IdentityInterface because Nipype cavalierly
        # disappears IdentityInterface output nodes.
        output_xfc = StickyIdentityInterface(fields=['out_dict'])
        output_spec = pe.Node(output_xfc, name='output_spec')
        mdl_wf.connect(assoc_node, 'out_dict', output_spec, 'out_dict')
        self.logger.debug("The modeling workflow output is %s with"
                           " fields %s" % (output_spec.name, out_fields))

        # Instrument the nodes for cluster submission, if necessary.
        self._configure_nodes(mdl_wf)

        self.logger.debug("Created the %s workflow." % mdl_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(mdl_wf)

        return mdl_wf

    def _create_ohsu_workflow(self, **opts):
        """
        Creates the modeling base workflow. This workflow performs the
        steps described in
        :class:`qipipe.pipeline.modeling.ModelingWorkflow` with the
        exception of XNAT upload.

        :Note: This workflow is adapted from the AIRC workflow at
        https://everett.ohsu.edu/hg/qin_dce. The AIRC workflow time
        series merge is removed and added as input to the workflow
        created by this method. The modeling optimization parameters
        are specified in the configuration rather than inferred
        from fastfit_cli.get_available_model to allow import of
        this Fastfit module in an environment which does not have
        the proprietary OHSU fastfit library.

        :param opts: the PK modeling parameters
        :return: the Nipype Workflow
        """
        workflow = pe.Workflow(name='ohsu', base_dir=self.base_dir)

        # The modeling profile configuration sections.
        self.profile_sections = OHSU_CONF_SECTIONS

        # The PK modeling parameters.
        r1_opts = self._r1_parameters(**opts)
        # Set the use_fixed_r1_0 flag.
        use_fixed_r1_0 = r1_opts.get('r1_0_val') != None

        # Set up the input node.
        non_r1_flds = ['time_series', 'mask', 'bolus_arrival_index']
        in_fields = non_r1_flds + r1_opts.keys()
        input_xfc = IdentityInterface(fields=in_fields)
        input_spec = pe.Node(input_xfc, name='input_spec')
        # Set the config parameters.
        for field in non_r1_flds:
            if field in opts:
                setattr(input_spec.inputs, field, opts[field])
        for field, value in r1_opts.iteritems():
            setattr(input_spec.inputs, field, value)

        # If we are not using a fixed r1_0 value, then compute a map
        # from a proton density weighted scan and the baseline of the
        # DCE series.
        if not use_fixed_r1_0:
            # Create the DCE baseline image.
            baseline_xfc = Function(
                input_names=['time_series', 'base_end'],
                output_names=['out_file'], function=make_baseline
            )
            baseline = pe.Node(baseline_xfc, name='baseline')
            workflow.connect(input_spec, 'time_series', baseline, 'time_series')
            workflow.connect(input_spec, 'base_end', baseline, 'base_end')
            # Create the R1_0 map.
            get_r1_0_xfc = Function(
                input_names=['pdw_file', 't1w_file', 'max_r1_0', 'mask'],
                output_names=['r1_0_map'], function=get_r1_0
            )
            get_r1_0 = pe.Node(get_r1_0_xfc, name='get_r1_0')
            workflow.connect(input_spec, 'pdw_file', get_r1_0, 'pdw_file')
            workflow.connect(baseline, 'out_file', get_r1_0, 't1w_file')
            workflow.connect(input_spec, 'max_r1_0', get_r1_0, 'max_r1_0')
            workflow.connect(input_spec, 'mask', get_r1_0, 'mask')

        # Convert the DCE time series to R1 maps.
        r1_series = pe.Node(DceToR1(), name='r1_series')
        workflow.connect(input_spec, 'time_series', r1_series, 'in_file')
        workflow.connect(input_spec, 'base_end', r1_series, 'base_end')
        workflow.connect(input_spec, 'mask', r1_series, 'mask')
        if use_fixed_r1_0:
            workflow.connect(input_spec, 'r1_0_val', r1_series, 'r1_0_val')
        else:
            raise ModelingError('The DceToR1 r1_0_map attribute is not'
                                ' yet supported')
            # TODO - remove error and enable below when r1_0_map is
            # supported.
            #workflow.connect(get_r1_0, 'r1_0_map', r1_series, 'r1_0')

        # Copy the time series meta-data to the R1 series.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        copy_meta.inputs.include_classes = [('global', 'const'),
                                            ('time', 'samples')]
        workflow.connect(input_spec, 'time_series', copy_meta, 'src_file')
        workflow.connect(r1_series, 'out_file', copy_meta, 'dest_file')

        # Get the pharmacokinetic mapping parameters.
        aif_shift_flds = ['time_series', 'bolus_arrival_index']
        aif_shift_xfc = Function(input_names=aif_shift_flds,
                                 output_names=['aif_shift'],
                                 function=get_aif_shift)
        aif_shift = pe.Node(aif_shift_xfc, name='get_aif_shift')
        workflow.connect(input_spec, 'time_series', aif_shift, 'time_series')
        workflow.connect(input_spec, 'bolus_arrival_index',
                         aif_shift, 'bolus_arrival_index')
        fit_params_flds = ['cfg_file', 'aif_shift']
        fit_params_xfc = Function(input_names=fit_params_flds,
                                  output_names=['params_csv'],
                                  function=get_fit_params)
        fit_params = pe.Node(fit_params_xfc, name='fit_params')
        fit_params.inputs.cfg_file = os.path.join(CONF_DIR, MODELING_CONF_FILE)
        workflow.connect(aif_shift, 'aif_shift', fit_params, 'aif_shift')

        # Work around the following Fastfit limitation:
        # * The Fastfit model_name, optimization_params and optional_outs
        #   inputs must be set before the Fastfit output is connected in
        #   the workflow.
        # These inputs are specified in the ModelingWorkflow configuration
        # named modeling.cfg. However, the configuration inputs are assigned
        # only in the WorkflowBase._run_workflow wrapper directly before
        # the workflow is run. The work-around is to get the configuration
        # settings and set the inputs here.
        fastfit_cfg = self.configuration['Fastfit']
        if not fastfit_cfg:
            raise ModelingError('The modeling configuration is missing the'
                                ' Fastfit topic')
        fastfit_opts = {opt: fastfit_cfg[opt] for opt in FASTFIT_CONF_PROPS
                        if opt in fastfit_cfg}
        # The pharmacokinetic model optimizer.
        fastfit = pe.Node(Fastfit(**fastfit_opts), name='fastfit')
        workflow.connect(copy_meta, 'dest_file', fastfit, 'in_file')
        workflow.connect(input_spec, 'mask', fastfit, 'mask')
        workflow.connect(fit_params, 'params_csv', fastfit, 'other_params_csv')

        # Compute the Ktrans difference.
        delta_k_trans = pe.Node(fsl.ImageMaths(), name='delta_k_trans')
        delta_k_trans.inputs.op_string = '-sub'
        workflow.connect(fastfit, 'k_trans', delta_k_trans, 'in_file')
        workflow.connect(fastfit, 'ext_tofts.k_trans',
                         delta_k_trans, 'in_file2')

        # The non-fastfit output fields.
        non_fastfit_outs = ['r1_series', 'params_csv', 'delta_k_trans']
        # The mandatory fastfit output fields.
        mandatory_outs = fastfit_opts.get('optimization_params', [])
        # The optional fastfit output fields.
        optional_outs = fastfit_opts.get('optional_outs', [])
        # All fastfit output fields.
        fastfit_outs = mandatory_outs + optional_outs
        # All upstream output fields.
        upsteam_outs = non_fastfit_outs + fastfit_outs

        # The FXL (Standard TOFTS) {upstream: output} dictionary.
        fxl_outs_dict = {fld: fld.replace(FXL_MODEL_PREFIX, 'fxl_')
                    for fld in optional_outs
                    if fld.startswith(FXL_MODEL_PREFIX)}
        # The corresponding FXR (Shutter Speed) {upstream: output}
        # dictionary.
        fxr_outs_dict = {fld.replace('fxl_', ''): fld.replace('fxl_', 'fxr_')
                         for fld in fxl_outs_dict.itervalues()}
        # The FXL/FXR {upstream: output} dictionary.
        complementary_outs_dict = fxl_outs_dict.copy()
        complementary_outs_dict.update(fxr_outs_dict)
        # The non-FXL/FXR outputs.
        other_fastfit_outs = [fld for fld in fastfit_outs
                              if not fld in complementary_outs_dict]
        print (">>mdl couts: %s oouts: %s" % (complementary_outs_dict, other_fastfit_outs))
        other_outs = non_fastfit_outs + other_fastfit_outs
        # The output fields.
        output_fields = (
            complementary_outs_dict.values() + other_outs
        )
        # The output node.
        output_spec = pe.Node(IdentityInterface(fields=output_fields),
                              name='output_spec')
        self.logger.debug("Created the %s workflow output node %s with output"
                          " fields:\n%s" %
                          (workflow.name, output_spec.name, output_fields))
        # Collect the non-fastfit outputs.
        workflow.connect(copy_meta, 'dest_file', output_spec, 'r1_series')
        workflow.connect(fit_params, 'params_csv', output_spec, 'params_csv')
        workflow.connect(delta_k_trans, 'out_file',
                         output_spec, 'delta_k_trans')
        # Collect the complementary fastfit outputs.
        for fastfit_fld, out_fld in complementary_outs_dict.iteritems():
            workflow.connect(fastfit, fastfit_fld, output_spec, out_fld)
        # Collect the other fastfit outputs.
        for fld in other_fastfit_outs:
            workflow.connect(fastfit, fld, output_spec, fld)

        self._configure_nodes(workflow)

        return workflow

    def _create_mock_workflow(self, **opts):
        """
        Creates a dummy modeling base workflow. This workflow performs
        the steps described in
        :class:`qipipe.pipeline.modeling.ModelingWorkflow` with the
        exception of XNAT upload.

        :param opts: the PK modeling parameters
        :return: the Nipype Workflow
        """
        workflow = pe.Workflow(name='mock', base_dir=self.base_dir)

        # The modeling profile configuration sections.
        self.profile_sections = []

        # The PK modeling parameters.
        opts = self._r1_parameters(**opts)

        # Set up the input node.
        non_pk_flds = ['time_series', 'mask', 'bolus_arrival_index']
        in_fields = non_pk_flds + opts.keys()
        input_xfc = IdentityInterface(fields=in_fields, **opts)
        input_spec = pe.Node(input_xfc, name='input_spec')
        # Set the config parameters.
        for field in in_fields:
            if field in opts:
                setattr(input_spec.inputs, field, opts[field])

        # Get the pharmacokinetic mapping parameters with a mock
        # AIF shift.
        fit_params_flds = ['cfg_file', 'aif_shift']
        fit_params_xfc = Function(input_names=fit_params_flds,
                                  output_names=['out_dict'],
                                  function=get_fit_params)
        fit_params = pe.Node(fit_params_xfc, name='fit_params')
        fit_params.inputs.cfg_file = os.path.join(CONF_DIR, MODELING_CONF_FILE)
        fit_params.inputs.aif_shift = 40.0
        # The mock pharmacokinetic model optimizer copies the mask.
        fastfit_flds = ['out_dict', 'fxr_k_trans']
        fastfit = pe.Node(IdentityInterface(fields=fastfit_flds), name='fastfit')
        workflow.connect(input_spec, 'mask', fastfit, 'fxr_k_trans')
        workflow.connect(fit_params, 'out_dict', fastfit, 'params_csv')

        # Collect the mock outputs.
        output_flds = fastfit_flds + ['pk_params']
        output_spec = pe.Node(IdentityInterface(fields=output_flds),
                              name='output_spec')
        workflow.connect(fastfit, 'fxr_k_trans', output_spec, 'fxr_k_trans')
        workflow.connect(fit_params, 'params_csv', output_spec, 'pk_params')

        self._configure_nodes(workflow)

        return workflow

    def _r1_parameters(self, **opts):
        """
        Collects the R1 modeling parameters defined in either the options
        or the configuration, as described in :class:`ModelingWorkflow`.

        :param opts: the input options
        :return: the parameter {name: value} dictionary
        """
        config = self.configuration.get('R1', {})
        # The R1_0 computation fields.
        r1_fields = ['pd_dir', 'max_r1_0']
        # All of the possible fields.
        fields = set(r1_fields)
        fields.update(['base_end', 'r1_0_val'])
        # The PK options.
        r1_opts = {k: opts[k] for k in fields if k in opts}
        if 'base_end' not in r1_opts:
            # Look for the the baseline parameter in the configuration.
            if 'base_end' in config:
                r1_opts['base_end'] = config['base_end']
            else:
                # The default baseline image count is 1.
                r1_opts['base_end'] = 1

        # Set the use_fixed_r1_0 variable to None, signifying unknown.
        use_fixed_r1_0 = None
        # Get the R1_0 parameter values.
        if 'r1_0_val' in r1_opts:
            r1_0_val = r1_opts.get('r1_0_val')
            if r1_0_val:
                use_fixed_r1_0 = True
            else:
                use_fixed_r1_0 = False
        else:
            for field in r1_fields:
                value = r1_opts.get(field)
                if value:
                    use_fixed_r1_0 = False

        # If none of the R1_0 options are set in the options,
        # then try the configuration.
        if use_fixed_r1_0 == None:
            r1_0_val = config.get('r1_0_val')
            if r1_0_val:
                r1_opts['r1_0_val'] = r1_0_val
                use_fixed_r1_0 = True

        # If R1_0 is not fixed, then augment the R1_0 options
        # from the configuration, if necessary.
        if not use_fixed_r1_0:
            for field in r1_fields:
                if field not in r1_opts and field in config:
                    use_fixed_r1_0 = False
                    r1_opts[field] = config[field]
                # Validate the R1 parameter.
                if not r1_opts.get(field):
                    raise ModelingError("Missing both the r1_0_val and the"
                                        " %s parameter." % field)

        # If the use_fixed_r1_0 flag is set, then remove the
        # extraneous R1 computation fields.
        if use_fixed_r1_0:
            for field in r1_fields:
                r1_opts.pop(field, None)

        self.logger.debug("The PK modeling parameters: %s" % r1_opts)

        return r1_opts


### Utility functions called by workflow nodes. ###

def create_profile(technique, source, configuration, sections, dest):
    """
    :meth:`qipipe.helpers.metadata.create_profile` wrapper.

    :param technique: the modeling technique
    :param source: the modeling input XNAT resource name
    :param configuration: the modeling workflow interface settings
    :param sections: the profile sections
    :param dest: the output profile file path
    """

    from qipipe.helpers import metadata

    # The correct technique names.
    TECHNIQUE_NAMES = dict(ohsu='OHSU', mock='Mock')

    technique = TECHNIQUE_NAMES.get(technique, technique)

    return metadata.create_profile(configuration, sections,
                                   dest=dest,
                                   general=dict(technique=technique),
                                   source=dict(resource=source))


def make_baseline(time_series, base_end):
    """
    Makes the R1_0 computation baseline NIfTI file.

    :param time_series: the modeling input 4D NIfTI image file path
    :param base_end: the exclusive limit of the baseline
        computation input series
    :return: the baseline NIfTI file name
    :raise ModelingError: if the end index is a negative number
    """
    import os
    import nibabel as nb
    from dcmstack.dcmmeta import NiftiWrapper

    if base_end <= 0:
        raise ModelingError("The R1_0 computation baseline end index"
                            " input value is not a positive number:"
                            " %s" % base_end)
    ts_nii = nb.load(time_series)
    ts_nw = NiftiWrapper(ts_nii)

    baselines = []
    for idx, split_nii in enumerate(ts_nw.split()):
        if idx == base_end:
            break
        baselines.append(split_nii)

    if len(baselines) == 1:
        baseline_nw = baselines[0]
    else:
        baseline_nw = NiftiWrapper.from_sequence(baselines)

    out_file = os.path.join(os.getcwd(), 'baseline.nii.gz')
    nb.save(baseline_nw, out_file)

    return out_file

def get_r1_0(pdw_file, t1w_file, max_r1_0, mask=None):
    """
    Returns the R1_0 map NIfTI file from the given proton density
    and T1-weighted images. The R1_0 map is computed using the
    ``pdw_t1w_to_r1`` function. The ``pdw_t1w_to_r1`` module
    must be in the Python path.

    :param pdw_file: the proton density NIfTI image file path
    :param t1w_file: the T1-weighted image file path
    :param max_r1_0: the R1_0 range maximum
    :param mask: the optional mask image file path to use
    :return: the R1_0 map NIfTI image file path
    """
    import os
    import nibabel as nb
    import numpy as np
    from pdw_t1w_to_r1 import pdw_t1w_to_r1
    from dcmstack.dcmmeta import NiftiWrapper

    pdw_nii = nb.load(pdw_file)
    pdw_nw = NiftiWrapper(pdw_nii, make_empty=True)
    t1w_nw = NiftiWrapper(nb.load(t1w_file), make_empty=True)
    r1_space = np.arange(0.01, max_r1_0, 0.01)
    pdw_opts = {}
    if mask:
        pdw_opts['mask'] = nb.load(mask).get_data()
    r1_0_arr = pdw_t1w_to_r1(pdw_nw, t1w_nw, r1_space=r1_space, **pdw_opts)

    cwd = os.getcwd()
    r1_0_nii = nb.Nifti1Image(r1_0_arr, pdw_nii.affine)
    out_file = os.path.join(cwd, 'r1_0_map.nii.gz')
    nb.save(r1_0_nii, out_file)

    return out_file


def get_aif_shift(time_series, bolus_arrival_index):
    """
    Calculates the arterial input function offset as:

    *t*\ :sub:`arrival` - *t*\ :sub:`0`

    where *t*\ :sub:`0` is the first slice acquisition time
    and *t*\ :sub:`arrival` averages the acquisition times at
    and immediately following bolus arrival.

    :param time_series: the modeling input 4D NIfTI image file path
    :param bolus_arrival_index: the bolus uptake series index
    :return: the parameter CSV file path
    """
    import os
    import csv
    import nibabel as nb
    import numpy as np
    from dcmstack.dcmmeta import NiftiWrapper
    from dcmstack import dcm_time_to_sec

    # Load the time series into a NIfTI wrapper.
    ts_nii = nb.load(time_series)
    ts_nw = NiftiWrapper(ts_nii)

    # The AIF shift parameter is the offset of the bolus arrival
    # series mid-point acquisition time from the MR session start
    # time.
    acq_time0 = dcm_time_to_sec(ts_nw.get_meta('AcquisitionTime', (0, 0, 0, 0)))
    acq_time1 = dcm_time_to_sec(
        ts_nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_arrival_index))
    )
    acq_time2 = dcm_time_to_sec(
        ts_nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_arrival_index + 1))
    )
    return ((acq_time1 + acq_time2) / 2.0) - acq_time0


def get_fit_params(cfg_file, aif_shift):
    """
    Obtains the following modeling fit parameters:

    * *aif_shift*: arterial input function parameter array
    * *aif_delta_t*: acquisition time deltas
    * *aif_shift*: acquisition time shift
    * *r1_cr*: contrast R1
    * *r1_b_pre*: pre-contrast R1

    The *aif_shift* is calculated by :func:`get_aif_shift` and passed
    to this function. The remaining parameters are read from the
    :const:`MODELING_CONF_FILE`.

    :param cfg_file: the modeling configuration file
    :return: the parameter CSV file path
    """
    import os
    import csv
    from qiutil.collections import is_nonstring_iterable
    from qiutil.ast_config import read_config
    from qipipe.pipeline.modeling import FASTFIT_PARAMS_FILE

    # The R1 parameters used by Fastfit.
    FASTFIT_R1_PARAMS = ['r1_cr', 'r1_b_pre']

    # The config parameters.
    cfg = read_config(cfg_file)
    cfg_dict = dict(cfg)
    # Start with the AIF parameters.
    fastfit_opts = cfg_dict.get('AIF').copy()
    # Add the R1 Fastfit parameters.
    r1_opts = cfg_dict.get('R1')
    for param in FASTFIT_R1_PARAMS:
        if param in r1_opts:
            fastfit_opts[param] = r1_opts[param]

    # Make CSV rows from the options.
    rows = []
    for key, value in fastfit_opts.iteritems():
        if is_nonstring_iterable(value):
            row = [key] + [str(v) for v in value]
        else:
            row = [key, str(value)]
        rows.append(row)
    # Add the shift.
    rows.append(['aif_shift', str(aif_shift)])

    # Create the parameter CSV file.
    with open(FASTFIT_PARAMS_FILE, 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(rows)

    return os.path.join(os.getcwd(), FASTFIT_PARAMS_FILE)


def associate(names, values):
    """
    Captures the synchronized *names* and *values* in a
    dictionary.

    :param names: the field names
    :param values: the field values
    :return: the target {name: value} dictionary
    """
    # The {name: value} dictionary cardinality.
    n = min(len(names), len(values))

    return {names[i]: values[i] for i in range(n)}
