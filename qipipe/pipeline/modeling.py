import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.dcmstack import (DcmStack, MergeNifti, CopyMeta)
from nipype.interfaces.utility import (IdentityInterface, Function)
from ..interfaces import (XNATUpload, Fastfit)
from ..helpers import file_helper
from ..helpers.project import project
from .workflow_base import WorkflowBase
from .distributable import DISTRIBUTABLE
from ..helpers.logging_helper import logger


PK_PREFIX = 'pk'
"""The XNAT modeling assessor object label prefix."""


def run(input_dict, **opts):
    """
    Creates a :class:`qipipe.pipeline.modeling.ModelingWorkflow` and runs it
    on the given inputs.
    
    :param input_dict: the :meth:`qipipe.pipeline.modeling.ModelingWorkflow.run`
        inputs
    :param opts: the :class:`qipipe.pipeline.modeling.ModelingWorkflow`
        initializer options
    :return: the :meth:`qipipe.pipeline.modeling.ModelingWorkflow.run` result
    """
    return ModelingWorkflow(**opts).run(input_dict)


class ModelingWorkflow(WorkflowBase):

    """
    The ModelingWorkflow builds and executes the Nipype pharmacokinetic
    mapping workflow.
    
    The workflow calculates the modeling parameters for input images as
    follows:
    
    - Compute the |R10| value, if it is not given in the options
    
    - Convert the DCE time series to a R1 map series
    
    - Determine the AIF and R1 fit parameters from the time series
    
    - Optimize the BOLERO pharmacokinetic model
    
    - Upload the modeling result to XNAT
    
    The modeling workflow input is the `input_spec` node consisting of the
    following input fields:
    
    - `subject`: the subject name
    
    - `session`: the session name
    
    - `mask`: the mask to apply to the images
    
    - `images`: the session images to model
    
    - the PK modeling parameters described in
      :meth:`qipipe.pipeline.modeling.ModelingWorkflow.__init__`
    
    If an input field is defined in the
    :meth:`qipipe.pipeline.modeling.ModelingWorkflow.__init__`
    configuration file ``Parameters`` topic, then the input field is set to
    that value.
    
    If the |R10| option is not set, then it is computed from the proton density
    weighted scans and DCE series baseline image.
    
    The outputs are collected in the `output_spec` node with the following
    fields:
    
    - `r1_series`: the R1 series files
    
    - `pk_params`: the AIF and R1 parameter CSV file
    
    - `k_trans`: the |Ktrans| extra/intravasation transfer rate
    
    - `v_e`: the |ve| interstitial volume fraction
    
    - `tau_i`: the intracellular |H2O| mean lifetime
    
    In addition, if |R10| is computed, then the output includes the
    following fields:
    
    - `pdw_image`: the proton density weighted image
    
    - `dce_baseline`: the DCE series baseline image
    
    - `r1_0`: the computed |R10| value
    
    This workflow is adapted from https://everett.ohsu.edu/hg/qin_dce.
    
    :Note: This workflow uses proprietary OHSU AIRC software, notably the BOLERO
        implementation of the `shutter speed model`_.
    
    .. reST substitutions:
    .. |H2O| replace:: H\ :sub:`2`\ O
    .. |Ktrans| replace:: K\ :sup:`trans`
    .. |ve| replace:: v\ :sub:`e`
    .. |R10| replace:: R1\ :sub:`0`
    
    .. _shutter speed model: http://www.ncbi.nlm.nih.gov/pmc/articles/PMC2582583
    """

    def __init__(self, **opts):
        """
        Initializes the modeling workflow. The modeling parameters can be
        defined in either the options or the configuration as follows:
        
        - The parameters can be defined in the configuration
          ``Parameters`` section.
        
        - The input options take precedence over the configuration
          settings.
        
        - The ``r1_0_val`` takes precedence over the R1_0 computation
          fields ``pd_dir`` and ``max_r1_0``. If ``r1_0_val`` is set
          in the input options, then ``pd_dir`` and ``max_r1_0`` are
          not included from the result.
        
        - If ``pd_dir`` and ``max_r1_0`` are set in the input options
          and ``r1_0_val`` is not set in the input options, then
          a ``r1_0_val`` configuration setting is ignored.
        
        - The ``baseline_end_idx`` defaults to 1 if it is not set in
          either the input options or the configuration.
        
        :param opts: the following initialization options:
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword r1_0_val: the optional fixed |R10| value
        :keyword max_r1_0: the maximum computed |R10| value, if the fixed |R10|
            option is not set
        :keyword pd_dir: the proton density files parent directory, if the fixed |R10|
            option is not set
        :keyword baseline_end_idx: the number of images to merge into a R1 series
            baseline image (default is 1)
        """
        cfg_file = opts.pop('cfg_file', None)
        super(ModelingWorkflow, self).__init__(logger(__name__), cfg_file)

        self.assessor = "%s_%s" % (PK_PREFIX, file_helper.generate_file_name())
        """
        The XNAT assessor name for all executions of this
        :class:`qipipe.pipeline.modeling.ModelingWorkflow` instance. The name
        is unique, which permits more than one model to be stored for each input
        series without a name conflict.
        """

        self.workflow = self._create_workflow(**opts)
        """
        The modeling workflow described in
        :class:`qipipe.pipeline.modeling.ModelingWorkflow`.
        """

    def run(self, input_dict, **opts):
        """
        Executes the modeling workflow described in
        :class:`qipipe.pipeline.modeling.ModelingWorkflow`
        on the given inputs.
        
        This run method connects the given inputs to the modeling workflow
        inputs. The execution workflow is then executed, resulting in a
        new uploaded XNAT analysis resource for each input session. This
        method returns the uploaded XNAT *(subject, session, analysis)* name
        tuples.
        
        :param input_dict: the input *{subject: {session: ([images], mask)}}*
            to model
        :param opts: the following workflow options:
        :keyword reconstruction: the XNAT reconstruction to model
        :return: the modeling result XNAT assessor name
        """
        # The number of sessions.
        sess_cnt = sum(map(len, input_dict.values()))
        # The [[(images, mask])]] list.
        sess_tuples = [sess_dict.values()
            for sess_dict in input_dict.itervalues()]
        # The flattened [(images, mask])] list.
        sbj_tuples = reduce(lambda x, y: x + y, sess_tuples)
        # The [images] list of lists.
        images_list = [images for images, _ in sbj_tuples]
        # The number of images.
        img_cnt = sum(map(len, images_list))

        # Model the images.
        self._logger.debug("Modeling %d images in %d sessions..." %
            (img_cnt, sess_cnt))
        for sbj, sess_dict in input_dict.iteritems():
            self._logger.debug("Modeling subject %s..." % sbj)
            for sess, sess_inputs in sess_dict.iteritems():
                images, mask = sess_inputs
                self._logger.debug("Modeling %d %s %s images..." %
                    (len(images), sbj, sess))
                self._model(sbj, sess, images, mask)
                self._logger.debug("Modeled %s %s." % (sbj, sess))
            self._logger.debug("Modeled subject %s." % sbj)
        self._logger.debug("Modeled %d images from %d sessions." %
            (img_cnt, sess_cnt))

        # Return the analysis name.
        return self.assessor

    def _model(self, subject, session, images, mask):
        """
        Runs the modeling workflow on the given session images.
        
        :param subject: the subject name
        :param session: the session name
        :param images: the input session images
        :param mask: the image mask
        """
        # Set the workflow input.
        self._set_modeling_input(self.workflow, subject, session, images,
            mask)
        # Execute the modeling workflow.
        self._logger.debug("Executing the %s workflow on %s %s..." %
            (self.workflow.name, subject, session))
        self._run_workflow(self.workflow)
        self._logger.debug("Executed the %s workflow on %s %s." %
            (self.workflow.name, subject, session))

    def _set_modeling_input(self, subject, session, images, mask):
        """
        Sets the modeling input.
        
        :param subject: the subject name
        :param session: the session name
        :param images: the images to model
        :param mask: the image mask
        """
        # Validate the mask input.
        if not mask:
            raise ValueError("The mask option is required for modeling.")

        # Set the workflow inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.images = images
        input_spec.inputs.mask = mask

    def _create_workflow(self, base_dir=None, **opts):
        """
        Builds the modeling workflow.
        
        :param base_dir: the execution working directory
            (default is a new temp directory)
        :param opts: the additional workflow initialization options
        :return: the Nipype workflow
        """
        self._logger.debug("Building the modeling workflow...")

        # The base workflow.
        if not base_dir:
            base_dir = tempfile.mkdtemp()
        reusable_wf = pe.Workflow(name='modeling', base_dir=base_dir)

        # Start with a base workflow.
        base_wf = self._create_base_workflow(base_dir=base_dir, **opts)

        # The workflow input fields.
        in_fields = ['subject', 'session', 'images', 'mask']
        input_xfc = IdentityInterface(fields=in_fields)
        input_spec = pe.Node(input_xfc, name='input_spec')
        self._logger.debug("The modeling workflow input is %s with"
            " fields %s" % (input_spec.name, in_fields))
        reusable_wf.connect(input_spec, 'mask', base_wf, 'input_spec.mask')
        reusable_wf.connect(input_spec, 'images', base_wf, 'input_spec.images')

        # The upload nodes.
        base_output = base_wf.get_node('output_spec')
        base_out_fields = base_output.outputs.copyable_trait_names()
        upload_dict = {field: self._create_upload_node(field)
            for field in base_out_fields}
        for field, node in upload_dict.iteritems():
            reusable_wf.connect(input_spec, 'subject', node, 'subject')
            reusable_wf.connect(input_spec, 'session', node, 'session')
            base_field = 'output_spec.' + field
            reusable_wf.connect(base_wf, base_field, node, 'in_files')

        # The output is the base outputs and the XNAT analysis name.
        out_fields = ['analysis'] + base_out_fields
        output_xfc = IdentityInterface(
            fields=out_fields, analysis=self.assessor)
        output_spec = pe.Node(output_xfc, name='output_spec')
        for field in base_out_fields:
            base_field = 'output_spec.' + field
            reusable_wf.connect(base_wf, base_field, output_spec, field)
        self._logger.debug("The modeling reusable workflow output is %s with"
            " fields %s" % (output_spec.name, out_fields))

        self._configure_nodes(reusable_wf)

        self._logger.debug("Created the %s workflow." % reusable_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self._depict_workflow(reusable_wf)

        return reusable_wf

    def _create_upload_node(self, resource):
        """
        :param resource: the modeling parameter resource name
        :return: the modeling parameter XNAT upload node
        """
        upload_xfc = XNATUpload(project=project(), assessor=self.assessor,
            resource=resource)
        name = 'upload_' + resource

        return pe.Node(upload_xfc, name=name)

    def _create_base_workflow(self, base_dir=None, **opts):
        """
        Creates the modeling base workflow. This workflow performs the steps
        described in :class:`qipipe.pipeline.modeling.ModelingWorkflow` with
        the exception of XNAT upload.
        
        :Note: This workflow is adapted from https://everett.ohsu.edu/hg/qin_dce.
        Any change to the ``qin_dce`` workflow should be reflected in this
        method.
        
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
        in_fields = ['images', 'mask'] + opts.keys()
        input_xfc = IdentityInterface(fields=in_fields, **opts)
        input_spec = pe.Node(input_xfc, name='input_spec')
        # Set the config parameters.
        for field in in_fields:
            if field in opts:
                setattr(input_spec.inputs, field, opts[field])

        # Merge the DCE data to 4D.
        dce_merge = pe.Node(MergeNifti(), name='dce_merge')
        dce_merge.inputs.out_format = 'dce_series'
        base_wf.connect(input_spec, 'images', dce_merge, 'in_files')

        # If we are not using a fixed r1_0 value, then compute a map from a
        # proton density weighted scan and the baseline of the DCE series.
        if not use_fixed_r1_0:
            # Convert each series dir to a Nifti.
            pd_stack = pe.Node(DcmStack(), name='pd_stack')
            pd_stack.inputs.embed_meta = True
            base_wf.connect(input_spec, 'pd_dir', pd_stack, 'dicom_files')

            # Create the DCE baseline image.
            make_base_func = Function(
                input_names=['dce_images', 'baseline_end_idx'],
                output_names=['baseline_nii'],
                function=_make_baseline),
            make_base = pe.Node(make_base_func, name='make_base')
            base_wf.connect(input_spec, 'images', make_base, 'dce_images')
            base_wf.connect(
                input_spec, 'baseline_end_idx', make_base, 'baseline_end_idx')

            # Create the R1_0 map.
            get_r1_0_func = Function(
                input_names=[
                    'pdw_image', 't1w_image', 'max_r1_0', 'mask_file'],
                output_names=['r1_0_map'],
                function=_make_r1_0),
            get_r1_0 = pe.Node(get_r1_0_func, name='get_r1_0')
            base_wf.connect(pd_stack, 'out_file', get_r1_0, 'pdw_image')
            base_wf.connect(make_base, 'baseline_nii', get_r1_0, 't1w_image')
            base_wf.connect(input_spec, 'max_r1_0', get_r1_0, 'max_r1_0')
            base_wf.connect(input_spec, 'mask', get_r1_0, 'mask_file')

        # Convert the DCE time series to R1 maps.
        get_r1_series_func = Function(
            input_names=['time_series', 'r1_0', 'baseline_end', 'mask_file'],
            output_names=['r1_series'], function=_make_r1_series)
        get_r1_series = pe.Node(get_r1_series_func, name='get_r1_series')
        base_wf.connect(dce_merge, 'out_file', get_r1_series, 'time_series')
        base_wf.connect(
            input_spec, 'baseline_end_idx', get_r1_series, 'baseline_end')
        base_wf.connect(input_spec, 'mask', get_r1_series, 'mask_file')
        if use_fixed_r1_0:
            base_wf.connect(input_spec, 'r1_0_val', get_r1_series, 'r1_0')
        else:
            base_wf.connect(get_r1_0, 'r1_0_map', get_r1_series, 'r1_0')

        # Copy the time series meta-data to the R1 series.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        copy_meta.inputs.include_classes = [
            ('global', 'const'), ('time', 'samples')]
        base_wf.connect(dce_merge, 'out_file', copy_meta, 'src_file')
        base_wf.connect(get_r1_series, 'r1_series', copy_meta, 'dest_file')

        # Get the pharmacokinetic mapping parameters.
        get_params_func = Function(
            input_names=['time_series'], output_names=['params_csv'],
            function=_get_fit_params)
        get_params = pe.Node(get_params_func, name='get_params')
        base_wf.connect(dce_merge, 'out_file', get_params, 'time_series')

        # Optimize the pharmacokinetic model.
        pk_map = pe.Node(Fastfit(), name='pk_map')
        pk_map.inputs.model_name = 'bolero_est'
        base_wf.connect(copy_meta, 'dest_file', pk_map, 'target_data')
        base_wf.connect(input_spec, 'mask', pk_map, 'mask')
        base_wf.connect(get_params, 'params_csv', pk_map, 'params_csv')
        # Set the MPI flag.
        pk_map.inputs.use_mpi = DISTRIBUTABLE

        # Set up the outputs.
        outputs = ['r1_series', 'pk_params', 'k_trans', 'v_e', 'tau_i']
        if not use_fixed_r1_0:
            outputs += ['pdw_image', 'dce_baseline', 'r1_0']
        output_spec = pe.Node(
            IdentityInterface(fields=outputs), name='output_spec')
        base_wf.connect(copy_meta, 'dest_file', output_spec, 'r1_series')
        base_wf.connect(get_params, 'params_csv', output_spec, 'pk_params')
        base_wf.connect(pk_map, 'k_trans', output_spec, 'k_trans')
        base_wf.connect(pk_map, 'v_e', output_spec, 'v_e')
        base_wf.connect(pk_map, 'tau_i', output_spec, 'tau_i')

        if not use_fixed_r1_0:
            base_wf.connect(pd_stack, 'out_file', output_spec, 'pdw_image')
            base_wf.connect(
                make_base, 'baseline_nii', output_spec, 'dce_baseline')
            base_wf.connect(get_r1_0, 'r1_0_map', output_spec, 'r1_0')

        self._configure_nodes(base_wf)

        return base_wf

    def _pk_parameters(self, **opts):
        """
        Collects the modeling parameters defined in either the options
        or the configuration as described in
        :meth:`qipipe.pipeline.modeling.ModelingWorkflow.__init__`.
        
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


def _make_baseline(dce_images, baseline_end_idx):
    from nipype.interfaces.dcmstack import MergeNifti
    from nipype.interfaces import fsl

    assert baseline_end_idx > 0
    if baseline_end_idx > 1:
        baseline_images = dce_images[:baseline_end_idx]
        merge_nii = MergeNifti()
        merge_nii.inputs.in_files = baseline_images
        merge_nii.inputs.out_format = 'baseline_merged'
        merged = merge_nii.run().outputs.out_file
        mean_image = fsl.MeanImage(name='mean_image')
        mean_image.inputs.in_file = merged
        return mean_image.run().outputs.out_file
    else:
        return dce_images[0]


def _make_r1_0(pdw_image, t1w_image, max_r1_0, mask_file, **kwargs):
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


def _make_r1_series(time_series, r1_0, mask_file, **kwargs):
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


def _get_fit_params(time_series):
    import os
    import csv
    import nibabel as nb
    import numpy as np
    from dcmstack.dcmmeta import NiftiWrapper
    from dcmstack import dcm_time_to_sec

    nii = nb.load(time_series)
    data = nii.get_data()
    n_vols = data.shape[-1]
    signal_means = np.array([np.mean(data[:,:,:, idx])
                             for idx in xrange(n_vols)])
    signal_diffs = np.diff(signal_means)

    # If we see a difference in average signal larger than double the .
    # difference from first two points, take that as bolus arrival.
    base_diff = np.abs(signal_diffs[0])
    for idx, diff_val in enumerate(signal_diffs[1:]):
        if diff_val > 2 * base_diff:
            bolus_idx = idx + 1
            break
    else:
        raise ValueError("Unable to determine bolus arrival")

    # Figure out the time in between bolus_idx and bolus_idx+1.
    nw = NiftiWrapper(nii)
    acq_time0 = dcm_time_to_sec(nw.get_meta('AcquisitionTime', (0, 0, 0, 0)))
    acq_time1 = dcm_time_to_sec(
        nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_idx)))
    acq_time2 = dcm_time_to_sec(
        nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_idx + 1)))
    aif_shift = ((acq_time1 + acq_time2) / 2.0) - acq_time0

    # Create parameter CSV.
    with open('params.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['aif_params', '0.674', '0.4', '2.2',
                             '0.23', '1.3', '0.09', '0.0013', '0.0'])
        csv_writer.writerow(['aif_delta_t', '1.5'])
        csv_writer.writerow(['aif_shift', str(aif_shift)])
        csv_writer.writerow(['r1_cr', '3.8'])
        csv_writer.writerow(['r1_b_pre', '0.71'])

    return os.path.join(os.getcwd(), 'params.csv')
