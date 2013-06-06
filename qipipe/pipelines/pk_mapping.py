import nipype.pipeline.engine as pe
from nipype.interfaces.dcmstack import DcmStack, MergeNifti, CopyMeta
from nipype.interfaces.utility import IdentityInterface, Function
from ..interfaces import XNATDownload, XNATUpload, Fastfit

def run(*session_specs, **opts):
    """
    Calculates the PK mapping parameters for the given registered images as follows:
    - Download the NiFTI mask and images from XNAT
    - Convert the DCE time series to a R1 map series
    - Get the pharmacokinetic mapping parameters
    - Perform the pharmacokinetic mapping
    - Upload the PK mapping result to XNAT
    
    The NiFTI scan images for each session are downloaded from XNAT into the
    ``scans`` subdirectory of the ``base_dir`` specified in the options
    (default is the current directory).
    
    The average is taken on the middle half of the NiFTI scan images.
    These images are averaged into a fixed reference template image.

    The options include the Pyxnat Workflow initialization options, as well as
    the following key => dictionary options:
    - ``mask``: the FSL ``mri_volcluster`` interface options
    - ``average``: the ANTS ``Average`` interface options
    - ``register``: the ANTS ``Registration`` interface options
    - ``reslice``: the ANTS ``ApplyTransforms`` interface options
    
    The registration applies an affine followed by a symmetric normalization transform.
    
    :param session_specs: the XNAT (subject, session) name tuples to register
    :param opts: the workflow options
    :return: the resliced XNAT (subject, session, reconstruction) designator tuples
    """

    # The work directory.
    work = opts.get('base_dir') or os.getcwd()
    # The scan image downloaad location.
    dest = os.path.join(work, 'scans')
    # Run the workflow on each session.
    recon_specs = [_register(sbj, sess, dest, **opts) for sbj, sess in session_specs]
    
    return recon_specs

def make_baseline(dce_images, baseline_end_idx):
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
    
def make_r1_0(pdw_image, t1w_image, max_r1_0, mask_file, **kwargs):
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
    
def make_r1_series(time_series, r1_0, mask_file, **kwargs):
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
    
def get_fit_params(time_series):
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
    
    #If we see a difference in average signal larger than double the 
    #difference from first two points, take that as bolus arrival
    base_diff = np.abs(signal_diffs[0])
    for idx, diff_val in enumerate(signal_diffs[1:]):
        if diff_val > 2 * base_diff:
            bolus_idx = idx + 1
            break
    else:
        raise ValueError("Unable to determine bolus arrival")
        
    #Figure out the time in between bolus_idx and bolus_idx+1
    nw = NiftiWrapper(nii)
    acq_time0 = dcm_time_to_sec(nw.get_meta('AcquisitionTime', (0, 0, 0, 0)))
    acq_time1 = dcm_time_to_sec(nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_idx)))
    acq_time2 = dcm_time_to_sec(nw.get_meta('AcquisitionTime', (0, 0, 0, bolus_idx+1)))
    aif_shift = ((acq_time1 + acq_time2) / 2.0) - acq_time0
    
    #Create parameter CSV
    with open('params.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['aif_params', '0.674', '0.4', '2.2', 
                             '0.23', '1.3', '0.09', '0.0013', '0.0'])
        csv_writer.writerow(['aif_delta_t', '1.5'])
        csv_writer.writerow(['aif_shift', str(aif_shift)])
        csv_writer.writerow(['r1_cr', '3.8'])
        csv_writer.writerow(['r1_b_pre', '0.71'])
    return path.join(os.getcwd(), 'params.csv')

def create_workflow(use_fixed_r1_0=False):
    '''Create the nipype workflow.'''
    workflow = pe.Workflow(name='pk_mapping')
    
    #Setup input node
    inputs = ['in_files', 'mask_file', 'baseline_end_idx', 'noise_thresh']
    if use_fixed_r1_0:
        inputs += ['r1_0_val']
    else:
        inputs += ['pd_dir', 'max_r1_0']
    inputspec = pe.Node(IdentityInterface(fields=inputs),
                        name='inputspec')
    
    #Merge the DCE data to 4D
    dce_merge = pe.Node(MergeNifti(), name='dce_merge')
    dce_merge.inputs.out_format = 'dce_series'
    workflow.connect(inputspec, 'in_files', dce_merge, 'in_files')
    
    #If we are not using a fixed val for r1_0, compute a map from a 
    #proton density weighted scan and the baseline of the DCE series
    if not use_fixed_r1_0:
        #Convert each series dir to a Nifti
        pd_stack = pe.Node(DcmStack(), name='pd_stack')
        pd_stack.inputs.embed_meta = True
        workflow.connect(inputspec, 'pd_dir',
                         pd_stack, 'dicom_files')
                     
        
        #Create the DCE baseline image
        make_base = pe.Node(Function(input_names=['dce_images',
                                                  'baseline_end_idx'],
                                     output_names=['baseline_nii'],
                                     function=make_baseline),
                            name='make_base')
        workflow.connect(dce_stack, 'out_file',
                         make_base, 'dce_images')
        workflow.connect(inputspec, 'baseline_end_idx',
                         make_base, 'baseline_end_idx')
                         
        #Create the R1_0 map
        get_r1_0 = pe.Node(Function(input_names=['pdw_image', 
                                                 't1w_image',
                                                 'max_r1_0',
                                                 'mask_file',
                                                ],
                                    output_names=['r1_0_map'],
                                    function=make_r1_0),
                           name='get_r1_0')
        workflow.connect(pd_stack, 'out_file', get_r1_0, 'pdw_image')
        workflow.connect(make_base, 'baseline_nii', get_r1_0, 't1w_image')
        workflow.connect(inputspec, 'max_r1_0', get_r1_0, 'max_r1_0')
        workflow.connect(inputspec, 'mask_file', get_r1_0, 'mask_file')
    
    #Convert DCE time series to series of R1 maps
    get_r1_series = pe.Node(Function(input_names=['time_series',
                                                  'r1_0',
                                                  'baseline_end',
                                                  'mask_file',
                                                 ],
                                     output_names=['r1_series'],
                                     function=make_r1_series),
                            name='get_r1_series')
    workflow.connect(dce_merge, 'out_file', 
                     get_r1_series, 'time_series')
    workflow.connect(inputspec, 'baseline_end_idx', 
                     get_r1_series, 'baseline_end')
    workflow.connect(inputspec, 'mask_file', get_r1_series, 'mask_file')
    if use_fixed_r1_0:
        workflow.connect(inputspec, 'r1_0_val', get_r1_series, 'r1_0')
    else:
        workflow.connect(get_r1_0, 'r1_0_map', get_r1_series, 'r1_0')
    
    #Copy meta data to the R1 series
    copy_meta = pe.Node(CopyMeta(), name='copy_meta')
    copy_meta.inputs.include_classes = [('global', 'const'), ('time', 'samples')]
    workflow.connect(dce_merge, 'out_file', copy_meta, 'src_file')
    workflow.connect(get_r1_series, 'r1_series', copy_meta, 'dest_file')
    
    #Get parameters for pharmacokinetic mapping
    get_params = pe.Node(Function(input_names=['time_series'],
                                  output_names=['params_csv'],
                                  function=get_fit_params),
                         name='get_params')
    workflow.connect(dce_merge, 'out_file', get_params, 'time_series')
    
    #Perform pharmacokinetic mapping
    pk_map = pe.Node(Fastfit(), name='pk_map')
    pk_map.inputs.model_name = 'bolero'
    workflow.connect(copy_meta, 'dest_file',
                     pk_map, 'target_data')
    workflow.connect(inputspec, 'mask_file',
                     pk_map, 'mask')
    workflow.connect(get_params, 'params_csv',
                     pk_map, 'params_csv')
    
    #Setup outputs
    outputs = ['r1_series', 
               'params_csv',
               'k_trans',
               'v_e',
               'tau_i'
              ]
    if not use_fixed_r1_0:
        outputs += ['pdw_image', 'dce_baseline', 'r1_0']
    outputspec = pe.Node(IdentityInterface(fields=outputs), name='outputspec')
    workflow.connect(copy_meta, 'dest_file', outputspec, 'r1_series')
    workflow.connect(get_params, 'params_csv', outputspec, 'params_csv')
    workflow.connect(pk_map, 'k_trans', outputspec, 'k_trans')
    workflow.connect(pk_map, 'v_e', outputspec, 'v_e')
    workflow.connect(pk_map, 'tau_i', outputspec, 'tau_i')
    
    if not use_fixed_r1_0:
        workflow.connect(pd_stack, 'out_file', outputspec, 'pdw_image')
        workflow.connect(make_base, 'baseline_nii', 
                         outputspec, 'dce_baseline')
        workflow.connect(get_r1_0, 'r1_0_map', outputspec, 'r1_0')
    
    return workflow
