import os
from collections import defaultdict
import nipype.pipeline.engine as pe
from nipype.interfaces.dcmstack import DcmStack, MergeNifti, CopyMeta
from nipype.interfaces.utility import IdentityInterface, Function
from ..interfaces import XNATDownload, XNATUpload, Fastfit
from ..helpers import file_helper
from ..helpers.project import project
from .workflow_base import WorkflowBase
from .distributable import DISTRIBUTABLE

import logging
logger = logging.getLogger(__name__)

PK_PREFIX = 'pk'
"""The XNAT modeling assessor object label prefix."""


def run(*inputs, **opts):
    """
    Creates a :class:`ModelingWorkflow` and runs its on the given inputs.
    
    :param inputs: the :meth:`ModelingWorkflow.run` inputs
    :param opts: the :class:`ModelingWorkflow` initializer and :meth:`ModelingWorkflow.run` options
    :return: the :meth:`ModelingWorkflow.run` result
    """
    recon = opts.pop('reconstruction', None)
    return ModelingWorkflow(**opts).run(*inputs, reconstruction=recon)


class ModelingWorkflow(WorkflowBase):
    """
    ModelingWorkflow builds and executes the Nipype pharmacokinetic mapping workflow.
    
    The workflow calculates the modeling parameters for input images as follows:
    
    - Compute the |R10| value, if it is not given in the options
    
    - Convert the DCE time series to a R1 map series
    
    - Determine the AIF and R1 fit parameters from the time series
    
    - Optimize the BOLERO pharmacokinetic model
    
    - Upload the modeling result to XNAT
    
    If the |R10| option is not set, then it is computed from
    the proton density weighted scans and DCE series baseline image.
    
    The outputs are collected in the ``output_spec`` node with the following
    fields:
    
    - ``r1_series``: the R1 series files
    
    - ``pk_params``: the AIF and R1 parameter CSV file
    
    - ``k_trans``: the |Ktrans| extra/intravasation transfer rate
    
    - ``v_e``: the |ve| interstitial volume fraction
    
    - ``tau_i``: the intracellular |H2O| mean lifetime
    
    In addition, if |R10| is computed, then the output includes the
    following fields:
    
    - ``pdw_image``: the proton density weighted image
    
    - ``dce_baseline``: the DCE series baseline image
    
    - ``r1_0``: the computed |R10| value
    
    This workflow is adapted from https://everett.ohsu.edu/hg/qin_dce.
    
    :Note: this workflow uses proprietary OHSU AIRC software, notably the BOLERO implementation
        of the `shutter speed model`_.
    
    .. reST substitutions:
    .. |H2O| replace:: H\ :sub:`2`\ O
    .. |Ktrans| replace:: K\ :sup:`trans`
    .. |ve| replace:: v\ :sub:`e`
    .. |R10| replace:: R1\ :sub:`0`
    
    .. _shutter speed model: http://www.ncbi.nlm.nih.gov/pmc/articles/PMC2582583
    """
    
    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow settings in
        that file override the default settings.
        
        :param opts: the following options
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword base_dir: the workflow execution directory (default current directory)
        :keyword r1_0_val: the optional fixed |R10| value
        :keyword max_r1_0: the maximum computed |R10| value, if the fixed |R10|
            option is not set
        :keyword pd_dir: the proton density files parent directory, if the fixed |R10|
            option is not set
        :keyword baseline_end_idx: the number of images to merge into a R1 series
            baseline image (default is 1)
        """
        super(ModelingWorkflow, self).__init__(logger, opts.pop('cfg_file', None))
        
        self.workflow = self._create_workflow(**opts)
        """
        The execution workflow.
        The workflow is executed by calling the :meth:`run` method.
        """
    
    def run(self, *inputs, **opts):
        """
        Builds the modeling workflow described in :meth:`create_workflow`
        and executes it on the given inputs.
        
        Each input is a (subject, session) tuple. The modeling input images
        to download are determined as follows:
        
        - If the ``reconstruction`` parameter is set, then the images for
        that reconstruction are downloaded. The reconstruction is typically
        a registration output.
        
        - Otherwise, the NiFTI scan series stack images are downloaded.
        
        Examples::
            
            modeling.run(('Breast003', 'Session02'))
            
            inputs = [('Sarcoma001', 'Session01'), ('Sarcoma001', 'Session02')]
            modeling.run(inputs, reconstruction='reg_Z4aUp8')
        
        This ``run`` method connects the given inputs to the modeling execution
        workflow inputs. The execution workflow is then executed, resulting in
        a new uploaded XNAT analysis resource for each input session. This
        method returns the uploaded XNAT (subject, session, analysis) name
        tuples.
        
        If the :module:`qipipe.pipelines.distributable ``DISTRIBUTABLE`` flag
        is set, then the execution is distributed using the `AIRC Grid Engine`_.
        
        .. |R10| replace:: R1\ :sub:`0`
        .. _AIRC Grid Engine: https://everett.ohsu.edu/wiki/GridEngine
        
        :param inputs: the (subject, scan) inputs
        :param opts: the following workflow options
        :keyword reconstruction: the XNAT reconstruction to model
        :return: the modeling XNAT (subject, session, analysis) tuples
        """
        # The workflow input node
        input_spec = self.workflow.get_node('input_spec')
        recon = opts.pop('reconstruction', None)
        if recon:
            input_spec.inputs.reconstruction = recon
        else:
            input_spec.inputs.container_type = 'scan'
        # The remaining PK modeling input factors.
        for field, value in opts.iteritems():
            setattr(input_spec.inputs, field, value)
        
        # The execution workflow iterates over the inputs.
        iter_dict = dict(subject=[], session=[])
        for sbj, sess in inputs:
            iter_dict['subject'].append(sbj)
            iter_dict['session'].append(sess)
        input_spec.iterables = iter_dict.items()
        
        # Collect the execution workflow output fields.
        exec_out_fields = ['subject', 'session', 'analysis']
        output_spec = pe.Node(IdentityInterface(fields=exec_out_fields, analysis=analysis),
            name='output_spec')
        for field in ['subject', 'session']:
            exec_wf.connect(input_spec, field, output_spec, field)
        
        # Run the workflow.
        self._run_workflow(workflow)
        
        # Return the (subject, session, analysis) tuples.
        return [(sbj, sess, analysis) for sbj, sess in inputs]
    
    def _create_workflow(self, base_dir=None, **opts):
        """
        Builds the modeling executable workflow described in :class:`ModelingWorkflow`.
        
        :param base_dir: the execution working directory (default is the current directory)
        :param opts: the additional reusable workflow options described in
            :meth:`__init__`
        :return: the modeling XNAT (subject, session, analysis) tuples
        """
        logger.debug("Building the modeling execution workflow...")
        
        # The reusable workflow.
        reusable_wf = self._create_reusable_workflow(base_dir=base_dir, **opts)
        
        # The execution workflow.
        wf_name = reusable_wf.name + '_exec'
        exec_wf = pe.Workflow(name=wf_name, base_dir=base_dir)
        
        # The download fields.
        dl_fields = ['subject', 'session', 'reconstruction', 'container_type']
        # The reusable workflow input fields.
        reusable_fields = reusable_wf.get_node('input_spec').inputs.copyable_trait_names()
        in_fields = set(dl_fields).union(reusable_fields)
        # The input node.
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # The image download node.
        dl_images = pe.Node(XNATDownload(project=project()), name='dl_images')
        for field in in_fields:
            exec_wf.connect(input_spec, field, dl_images, field)
        
        # Download the mask.
        dl_mask = pe.Node(XNATDownload(project=project(), reconstruction='mask'),
            name='dl_mask')
        exec_wf.connect(input_spec, 'subject', dl_mask, 'subject')
        exec_wf.connect(input_spec, 'session', dl_mask, 'session')
        
        # Model the images.
        exec_wf.connect(input_spec, 'subject', reusable_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', reusable_wf, 'input_spec.session')
        exec_wf.connect(dl_mask, 'out_file', reusable_wf, 'input_spec.mask_file')
        exec_wf.connect(dl_images, 'out_files', reusable_wf, 'input_spec.in_files')
        
        # Make the default XNAT assessment name. The name is unique, which permits
        # more than one model to be stored for each input series without a name
        # conflict.
        analysis = "%s_%s" % (PK_PREFIX, file_helper.generate_file_name())
        
        # The upload nodes.
        reusable_out_fields = reusable_wf.get_node('output_spec').outputs.copyable_trait_names()
        upload_node_dict = {field: _create_output_upload_node(analysis, field)
            for field in reusable_out_fields}
        for field, node in upload_node_dict.iteritems():
            exec_wf.connect(input_spec, 'subject', node, 'subject')
            exec_wf.connect(input_spec, 'session', node, 'session')
            reusable_field = 'output_spec.' + field
            exec_wf.connect(reusable_wf, reusable_field, node, 'in_files')
        
        # Collect the execution workflow output fields.
        exec_out_fields = ['subject', 'session', 'analysis']
        output_spec = pe.Node(IdentityInterface(fields=exec_out_fields, analysis=analysis),
            name='output_spec')
        for field in ['subject', 'session']:
            exec_wf.connect(input_spec, field, output_spec, field)
        
        logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            fname = "%s.dot" % exec_wf.name
            if exec_wf.base_dir:
                grf = os.path.join(exec_wf.base_dir, fname)
            else:
                grf = fname
            exec_wf.write_graph(dotfilename=grf)
            logger.debug("The %s workflow graph is depicted at %s.png." %
                (exec_wf.name, grf))
        
        return exec_wf
    
    def _create_output_upload_node(self, analysis, resource):
        """
        :param analysis: the modeling assessment name
        :param resource: the modeling parameter resource name
        :return: the modeling parameter XNAT upload node
        """
        name = 'upload_' + resource
        
        return pe.Node(XNATUpload(project=project(), assessor=analysis, resource=resource),
            name=name)
    
    def _create_reusable_workflow(self, base_dir=None, **opts):
        """
        Creates the modeling reusable workflow described in :class:`ModelingWorkflow`.
        
        :param base_dir: the workflow working directory
        :param opts: the additional PK mapping parameters described in :meth:`__init__`
        :return: the pyxnat Workflow
        """
        workflow = pe.Workflow(name='modeling', base_dir=base_dir)
        
        # The parameters can be defined in either the options or the configuration.
        config = self.configuration.get('Parameters', {})
        
        if  'baseline_end_idx' not in opts:
            # Look for the the baseline parameter in the configuration.
            if 'baseline_end_idx' in config:
                opts['baseline_end_idx'] = config['baseline_end_idx']
            else:
                # The default baseline image count.
                opts['baseline_end_idx'] = 1
        
        # The R1_0 computation fields.
        r1_fields = ['pd_dir', 'max_r1_0']
        # Mark the use_fixed_r1_0 variable as Unknown.
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
        
        # Validate the R1 parameters.
        if not use_fixed_r1_0:
            for field in r1_fields:
                if not opts.get(field):
                    raise ValueError("Missing both the r1_0_val and the %s"
                        " parameter." % field)
        
        logger.debug("The PK modeling parameters: %s" % opts)
        
        # Set up the input node.
        in_fields = ['subject', 'session', 'in_files', 'mask_file', 'baseline_end_idx']
        if use_fixed_r1_0:
            in_fields += ['r1_0_val']
        else:
            in_fields += r1_fields
        input_spec = pe.Node(IdentityInterface(fields=in_fields, **opts),
            name='input_spec')
        
        # Merge the DCE data to 4D.
        dce_merge = pe.Node(MergeNifti(), name='dce_merge')
        dce_merge.inputs.out_format = 'dce_series'
        workflow.connect(input_spec, 'in_files', dce_merge, 'in_files')
        
        # If we are not using a fixed r1_0 value, then compute a map from a
        # proton density weighted scan and the baseline of the DCE series.
        if not use_fixed_r1_0:
            # Convert each series dir to a Nifti.
            pd_stack = pe.Node(DcmStack(), name='pd_stack')
            pd_stack.inputs.embed_meta = True
            workflow.connect(input_spec, 'pd_dir',
                             pd_stack, 'dicom_files')
            
            # Create the DCE baseline image.
            make_base = pe.Node(Function(input_names=['dce_images',
                                                      'baseline_end_idx'],
                                         output_names=['baseline_nii'],
                                         function=_make_baseline),
                                name='make_base')
            workflow.connect(input_spec, 'in_files',
                             make_base, 'dce_images')
            workflow.connect(input_spec, 'baseline_end_idx',
                             make_base, 'baseline_end_idx')
            
            # Create the R1_0 map.
            get_r1_0 = pe.Node(Function(input_names=['pdw_image',
                                                     't1w_image',
                                                     'max_r1_0',
                                                     'mask_file',
                                                    ],
                                        output_names=['r1_0_map'],
                                        function=_make_r1_0),
                               name='get_r1_0')
            workflow.connect(pd_stack, 'out_file', get_r1_0, 'pdw_image')
            workflow.connect(make_base, 'baseline_nii', get_r1_0, 't1w_image')
            workflow.connect(input_spec, 'max_r1_0', get_r1_0, 'max_r1_0')
            workflow.connect(input_spec, 'mask_file', get_r1_0, 'mask_file')
        
        # Convert the DCE time series to R1 maps.
        get_r1_series = pe.Node(Function(input_names=['time_series',
                                                      'r1_0',
                                                      'baseline_end',
                                                      'mask_file',
                                                     ],
                                         output_names=['r1_series'],
                                         function=_make_r1_series),
                                name='get_r1_series')
        workflow.connect(dce_merge, 'out_file',
                         get_r1_series, 'time_series')
        workflow.connect(input_spec, 'baseline_end_idx',
                         get_r1_series, 'baseline_end')
        workflow.connect(input_spec, 'mask_file', get_r1_series, 'mask_file')
        if use_fixed_r1_0:
            workflow.connect(input_spec, 'r1_0_val', get_r1_series, 'r1_0')
        else:
            workflow.connect(get_r1_0, 'r1_0_map', get_r1_series, 'r1_0')
        
        # Copy the time series meta-data to the R1 series.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        copy_meta.inputs.include_classes = [('global', 'const'), ('time', 'samples')]
        workflow.connect(dce_merge, 'out_file', copy_meta, 'src_file')
        workflow.connect(get_r1_series, 'r1_series', copy_meta, 'dest_file')
        
        # Get the pharmacokinetic mapping parameters.
        get_params = pe.Node(Function(input_names=['time_series'],
                                      output_names=['params_csv'],
                                      function=_get_fit_params),
                             name='get_params')
        workflow.connect(dce_merge, 'out_file', get_params, 'time_series')
        
        # Optimize the pharmacokinetic model.
        pk_map = pe.Node(Fastfit(), name='pk_map')
        pk_map.inputs.model_name = 'bolero'
        workflow.connect(copy_meta, 'dest_file',
                         pk_map, 'target_data')
        workflow.connect(input_spec, 'mask_file',
                         pk_map, 'mask')
        workflow.connect(get_params, 'params_csv',
                         pk_map, 'params_csv')
        # Set the distributable MPI parameters.
        if DISTRIBUTABLE and 'FastFit' in config:
            qsub_args = config['FastFit'].get('qsub_args', {})
            if qsub_args:
                pk_map.inputs.use_mpi = True
                pk_map.plugin_args = dict(qsub_args=qsub_args, overwrite=True)
                logger.debug("FastFit MPI parameters: %s" % qsub_args)
        
        # Set up the outputs.
        outputs = ['r1_series',
                   'pk_params',
                   'k_trans',
                   'v_e',
                   'tau_i'
                  ]
        if not use_fixed_r1_0:
            outputs += ['pdw_image', 'dce_baseline', 'r1_0']
        output_spec = pe.Node(IdentityInterface(fields=outputs), name='output_spec')
        workflow.connect(copy_meta, 'dest_file', output_spec, 'r1_series')
        workflow.connect(get_params, 'params_csv', output_spec, 'pk_params')
        workflow.connect(pk_map, 'k_trans', output_spec, 'k_trans')
        workflow.connect(pk_map, 'v_e', output_spec, 'v_e')
        workflow.connect(pk_map, 'tau_i', output_spec, 'tau_i')
        
        if not use_fixed_r1_0:
            workflow.connect(pd_stack, 'out_file', output_spec, 'pdw_image')
            workflow.connect(make_base, 'baseline_nii',
                             output_spec, 'dce_baseline')
            workflow.connect(get_r1_0, 'r1_0_map', output_spec, 'r1_0')
        
        return workflow


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
    from os import path
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
    out_fn = path.join(cwd, 'r1_0_map.nii.gz')
    nb.save(out_nii, out_fn)
    return out_fn

def _make_r1_series(time_series, r1_0, mask_file, **kwargs):
    import os
    from os import path
    import nibabel as nb
    import numpy as np
    from dce_to_r1 import dce_to_r1
    from dcmstack.dcmmeta import NiftiWrapper
    
    dce_nw = NiftiWrapper(nb.load(time_series), make_empty=True)
    if not isinstance(r1_0, float):
        r1_0 = nb.load(r1_0).get_data()
    mask = nb.load(mask_file).get_data()
    r1_series = dce_to_r1(dce_nw, r1_0, mask=mask, **kwargs)
    
    cwd = os.getcwd()
    out_nii = nb.Nifti1Image(r1_series, dce_nw.nii_img.get_affine())
    out_fn = path.join(cwd, 'r1_series.nii.gz')
    nb.save(out_nii, out_fn)
    return out_fn

def _get_fit_params(time_series):
    import os, csv
    from os import path
    import nibabel as nb
    import numpy as np
    from dcmstack.dcmmeta import NiftiWrapper
    from dcmstack import dcm_time_to_sec
    
    nii = nb.load(time_series)
    data = nii.get_data()
    n_vols = data.shape[-1]
    signal_means = np.array([np.mean(data[:,:,:,idx])
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
    acq_time1 = dcm_time_to_sec(nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_idx)))
    acq_time2 = dcm_time_to_sec(nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_idx+1)))
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
    return path.join(os.getcwd(), 'params.csv')
