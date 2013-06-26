import os, distutils
from collections import defaultdict
import nipype.pipeline.engine as pe
from nipype.interfaces.dcmstack import DcmStack, MergeNifti, CopyMeta
from nipype.interfaces.utility import IdentityInterface, Function
from ..interfaces import XNATDownload, XNATUpload, Fastfit
from ..helpers import file_helper
from ..helpers.globals import PROJECT

PK_PREFIX = 'pk'
"""The XNAT modeling assessment object label prefix."""

DISTRIBUTABLE = not not distutils.spawn.find_executable('qsub')
"""Flag indicating whether the workflow can be distributed over a cluster."""

def run(*inputs, **opts):
    """
    Builds the modeling workflow described in :meth:`create_workflow`
    and executes it on the given inputs.

    Each input identifies an XNAT reconstruction or scan. A reconstruction
    input is a dictionary with keys ``subject``, ``session`` and
    ``reconstruction``. A scan input is a dictionary with keys ``subject``,
    ``session`` and ``scan``. The input dictionary values are the XNAT
    labels for the respective XNAT objects.
    
    The NiFTI session mask and input image files are downloaded from XNAT
    into the ``input`` subdirectory of the execution working directory.
    
    The modeling workflow is then built as described in :meth:create_workflow
    and executed. The result is uploaded to an XNAT analysis resource.
    
    If the ``qsub`` executable is found in the execution environment, then the
    execution is distributed on the cluster using the Sun Grid Engine.

    :param inputs: the XNAT input reconstruction or scan XNAT inputs
    :param base_dir: the execution working directory (default is the current directory)
    :param opts: the optional workflow inputs described in :meth:create_workflow 
    :return: the modeling XNAT (subject, session, analysis) tuples
    """
    # The workflow directory.
    base_dir = opts.pop('base_dir', os.getcwd())
    # The reusable workflow.
    reusable_wf = create_workflow(base_dir=base_dir, **opts)
    # The image download location.
    dest = os.path.join(base_dir, 'data')
    
    # Make the execution workflow.
    exec_wf = pe.Workflow(name='_'.join(reusable_wf.name(), 'exec'))
    
    # The execution workflow inputs.
    input_list_dict = defaultdict(list)
    for spec in input_specs:
        for k, v in spec.iteritems():
            input_list_dict[k].append(v)
    in_fields = input_list_dict.keys()
    input_spec = pe.Node(IdentityInterface(fields=in_fields))
    input_spec.iterables = input_list_dict
    
    # Download the mask.
    dl_mask = XNATDownload(project='QIN', reconstruction='mask', dest=dest)
    exec_wf.connect(input_spec, 'subject', dl_mask, 'subject')
    exec_wf.connect(input_spec, 'session', dl_mask, 'session')
    
    # Download the images.
    dl_images = XNATDownload(project='QIN', dest=dest)
    for field in in_fields:
        exec_wf.connect(input_spec, field, dl_images, field)

    # Model the images.
    exec_wf.connect(input_spec, 'subject', reusable_wf, 'input_spec.subject')
    exec_wf.connect(input_spec, 'session', reusable_wf, 'input_spec.session')
    exec_wf.connect(dl_mask, 'out_file', reusable_wf, 'input_spec.mask_file')
    exec_wf.connect(dl_images, 'out_files', reusable_wf, 'input_spec.in_files')
    
    # Upload the R1 series to XNAT.
    upload_r1 = pe.Node(XNATUpload(project=PROJECT, resource='r1_series', format='NIFTI'),
        name='upload_r1')
    exec_wf.connect(input_spec, 'subject', upload_r1, 'subject')
    exec_wf.connect(input_spec, 'session', upload_r1, 'session')
    exec_wf.connect(input_spec, 'analysis', upload_r1, 'analysis')
    exec_wf.connect(reusable_wf, 'output_spec.r1_series', upload_r1, 'in_files')
    
    # TODO - Upload the remaining outputs to XNAT.

    # Collect the workflow output fields.
    out_fields = ['subject', 'session', 'analysis']
    output_spec = pe.Node(IdentityInterface(fields=out_fields))
    for field in out_fields:
        exec_wf.connect(input_spec, field, output_spec, field)

    # Check whether the workflow can be distributed.
    if DISTRIBUTABLE:
        exec_wf.config['execution'] = {'job_finished_timeout': 60.0}
        args = dict(plugin='SGE',
                    plugin_args={'qsub_args' : '-l h_rt=1:00:00,mf=3G,h_vmem=3.5G -b n'})
    else:
        args = {}
    
    # Run the workflow.
    result = exec_wf.run(**args)

    # Return the (subject, session, analysis) tuples.
    return tuple([getattr(result.outputs.output_spec, field) for field in out_fields]) 
    
def create_workflow(base_dir=None, **inputs):
    """
    Creates the Nipype pharmacokinetic mapping workflow.
    
    .. reST substitutions:
    .. |H2O| replace:: H\ :sub:`2`\ O
    .. |Ktrans| replace:: K\ :sup:`trans`
    .. |ve| replace:: v\ :sub:`e`
    .. |R10| replace:: R1\ :sub:`0`
    
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

    - ``params_csv``: the AIF and R1 parameter CSV file

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
    
    .. _`shutter speed model` http://www.ncbi.nlm.nih.gov/pmc/articles/PMC2582583/?tool=pubmed
    
    :param inputs: the optional workflow ``input_spec`` node inputs listed below
    :keyword r1_0_val: the optional fixed |R10| value
    :keyword baseline_end_idx: the number of images to merge into a R1 series baseline image
        (default is 1)
    :keyword max_r1_0: the maximum computed |R10| value, if |R10| is not fixed
    :return: the pyxnat Workflow
    """
    workflow = pe.Workflow(name='modeling')
    
    # Make the default XNAT assessment object label, if necessary. The label is unique, which
    # permits more than one modeling to be stored for each input series without a name conflict.
    if 'analysis' not in inputs:
        inputs['analysis'] = "%s_%s" % (PK_PREFIX, file_helper.generate_file_name())
    
    # The default baseline image count.
    if 'baseline_end_idx' not in inputs:
        inputs['baseline_end_idx'] = 1
    
    # Set up the input node.
    in_fields = ['subject', 'session', 'analysis', 'in_files', 'mask_file', 'baseline_end_idx']
    if opts.get('r1_0_val'):
        in_fields += ['r1_0_val']
        use_fixed_r1_0 = True
    else:
        in_fields += ['pd_dir', 'max_r1_0']
        use_fixed_r1_0 = False
    input_spec = pe.Node(IdentityInterface(fields=in_fields, **inputs),
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
    
    if DISTRIBUTABLE:
        pk_map.inputs.use_mpi = True
        pk_map.plugin_args = {'qsub_args': '-pe mpi 48-120 -l h_rt=4:00:00,mf=750M,h_vmem=3G -b n', 
                               'overwrite' : True
                              }
    
    # Set up the outputs.
    outputs = ['r1_series', 
               'params_csv',
               'k_trans',
               'v_e',
               'tau_i'
              ]
    if not use_fixed_r1_0:
        outputs += ['pdw_image', 'dce_baseline', 'r1_0']
    output_spec = pe.Node(IdentityInterface(fields=outputs), name='output_spec')
    workflow.connect(copy_meta, 'dest_file', output_spec, 'r1_series')
    workflow.connect(get_params, 'params_csv', output_spec, 'params_csv')
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
