import os
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import AverageImages, ApplyTransforms
from nipype.interfaces.dcmstack import CopyMeta
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import DcmStack, MergeNifti, CopyMeta
from nipype.interfaces.utility import Select, IdentityInterface, Function
from .. import PROJECT
from ..interfaces import XNATDownload, XNATUpload, MriVolCluster
from ..helpers import xnat_helper
from ..helpers import file_helper

import logging
logger = logging.getLogger(__name__)

DEF_MASK_CLUSTER_OPTS = dict(
    max_thresh=10,
    min_voxels = 10000
)

DEF_ANTS_AVG_OPTS = dict(
    dimension=3,
    normalize=True
)
"""The default ANTS AverageImages options."""

DEF_ANTS_REG_OPTS = dict(
    dimension=3,
    transforms=['Affine', 'SyN'],
    transform_parameters=[(2.0,), (0.25, 3.0, 0.0)],
    number_of_iterations=[[1500, 200], [100, 50, 30]],
    write_composite_transform=True,
    collapse_output_transforms=False,
    metric=['Mattes']*2,
    metric_weight=[1]*2,
    radius_or_number_of_bins=[200]*2,
    sampling_strategy=['Regular', None],
    sampling_percentage=[0.10, None],
    convergence_threshold=[1.e-8, 1.e-9],
    convergence_window_size=[100]*2,
    smoothing_sigmas=[[4,2], [4,2,0]],
    shrink_factors=[[1,1], [3,2,1]],
    use_estimate_learning_rate_once=[True, True],
    output_transform_prefix='xfm',
    output_warped_image='warp.nii.gz'
)
"""The default ANTS Registration options."""

DEF_ANTS_WARP_OPTS = dict(
    dimension=3,
    interpolation='Linear',
    default_value=0,
    invert_transform_flags=[False, False]
)
"""The default ANTS ApplyTransforms options."""

REG_PREFIX = 'reg'
"""The XNAT registration reconstruction name prefix."""

def run(*session_specs, **opts):
    """
    Registers the scan NiFTI images for the given sessions as follows:
        - Download the NiFTI scans from XNAT
        - Make a mask to subtract extraneous tissue
        - Mask each input scan
        - Make a template by averaging the masked images
        - Create an affine and non-rigid transform for each image
        - Reslice the masked image with the transforms
        - Upload the mask and the resliced images
    
    The NiFTI scan images for each session are downloaded from XNAT into the
    C{scans} subdirectory of the C{base_dir} specified in the options
    (default is the current directory).
    
    The average is taken on the middle half of the NiFTI scan images.
    These images are averaged into a fixed reference template image.

    The options include the Pyxnat Workflow initialization options, as well as
    the following key => dictionary options:
        - C{Mask}: the FSL C{mri_volcluster} interface options
        - C{Average}: the ANTS C{Average} interface options
        - C{Register}: the ANTS C{Registration} interface options
        - C{Reslice}: the ANTS C{ApplyTransforms} interface options
    
    The registration applies an affine followed by a symmetric normalization transform.
    
    @param session_specs: the XNAT (subject, session) name tuples to register
    @param opts: the workflow options
    @return: the resliced XNAT (subject, session, reconstruction) designator tuples
    """

    # The work directory.
    work = opts.get('base_dir') or os.getcwd()
    # The scan image downloaad location.
    dest = os.path.join(work, 'scans')
    # Run the workflow on each session.
    recon_specs = [_register(sbj, sess, dest, **opts) for sbj, sess in session_specs]
    
    return recon_specs

def _register(subject, session, dest, **opts):
    """
    Builds and runs the registration workflow.
    
    @param subject: the XNAT subject label
    @param session: the XNAT session label
    @param dest: the scan download directory
    @param opts: the workflow options
    @return: the warpd XNAT (subject, session, reconstruction) designator tuple
    """
    # Download the scan images.
    tgt = os.path.join(dest, subject, session)
    images = _download_scans(subject, session, tgt)
    
    # Make a unique registration reconstruction name. This permits more than one
    # registration to be stored for each input image without a name conflict.
    recon = _generate_name(REG_PREFIX)
    
    
    # Make the workflow
    workflow = _create_workflow(subject, session, recon, images, **opts)
    # Execute the workflow
    workflow.run()
    
    # Return the recon specification.
    return (subject, session, recon)

def _download_scans(subject, session, dest):
    """
    Download the NIFTI scan files for the given session.
    
    @param subject: the XNAT subject label
    @param session: the XNAT session label
    @param dest: the destination directory path
    @return: the download file paths
    """

    with xnat_helper.connection() as xnat:
        return xnat.download(PROJECT, subject, session, dest=dest, container_type='scan', format='NIFTI')

def _create_workflow(subject, session, recon, images, **opts):
    """
    Creates the Pyxnat Workflow for the given session images.
    
    @param subject: the XNAT subject label
    @param session: the XNAT session label
    @param recon: the XNAT registration reconstruction label
    @param images: the input session scan NiFTI stacks
    @return: the registration workflow
    """
    msg = 'Creating the %s %s registration workflow' % (subject, session)
    if opts:
        msg = msg + ' with options %s' % opts
    logger.debug("%s...", msg)

    # The mask step options.
    mask_opts = opts.pop('Mask', {})
    # The average step options.
    avg_opts = opts.pop('Average', {})
    # The registration step options.
    reg_opts = opts.pop('Register', {})
    # The warp step options.
    rsmpl_opts = opts.pop('Warp', {})

    workflow = pe.Workflow(name='register', **opts)

    # The workflow input image iterator.
    input_spec = pe.Node(IdentityInterface(fields=['image']), name='input_spec')
    input_spec.inputs.subject = subject
    input_spec.inputs.session = session
    input_spec.iterables = dict(image=images).items()
    
    # Merge the DCE data to 4D.
    dce_merge = pe.Node(MergeNifti(), name='dce_merge')
    dce_merge.inputs.out_format = 'dce_series'
    dce_merge.inputs.in_files = images

    # Get a mean image from the DCE data.
    dce_mean = pe.Node(fsl.MeanImage(), name='dce_mean')
    workflow.connect(dce_merge, 'out_file', dce_mean, 'in_file')

    # Find the center of gravity from the mean image.
    find_cog = pe.Node(fsl.ImageStats(), name='find_cog')
    find_cog.inputs.op_string = '-C'
    workflow.connect(dce_mean, 'out_file', find_cog, 'in_file')

    # Zero everything posterior to the center of gravity on mean image.
    crop_back = pe.Node(fsl.ImageMaths(), name='crop_back')
    workflow.connect(dce_mean, 'out_file', crop_back, 'in_file')
    workflow.connect(find_cog, ('out_stat', _gen_crop_op_string), crop_back, 'op_string')
    # crop_back = pe.Node(Function(input_names=['image', 'cog'],
    #                              output_names=['cropped'],
    #                              function=_crop_posterior), 
    #                     name='crop_back')
    # workflow.connect(dce_mean, 'out_file', crop_back, 'image')
    # workflow.connect(find_cog, 'out_stat', crop_back, 'cog')

    # Find large clusters of empty space on the cropped image.
    cluster_mask_xf = _create_mask_cluster_interface(**mask_opts)
    cluster_mask = pe.Node(cluster_mask_xf, name='cluster_mask')
    workflow.connect(crop_back, 'out_file', cluster_mask, 'in_file')

    # Convert the cluster labels to binary mask.
    binarize = pe.Node(fsl.BinaryMaths(), name='binarize')
    binarize.inputs.operation = 'min'
    binarize.inputs.operand_value = 1
    workflow.connect(cluster_mask, 'out_cluster_file', binarize, 'in_file')

    # Invert the binary mask.
    inv_mask = pe.Node(fsl.maths.MathsCommand(), name='inv_mask')
    inv_mask.inputs.args = '-sub 1 -mul -1'
    inv_mask.inputs.out_file = "%s_%s.nii.gz" % (session.lower(), 'mask')
    workflow.connect(binarize, 'out_file', inv_mask, 'in_file')
    
    # Upload the mask to XNAT.
    upload_mask = pe.Node(XNATUpload(project=PROJECT, reconstruction='mask', format='NIFTI'),
        name='upload_mask')
    upload_mask.inputs.subject = subject
    upload_mask.inputs.session = session
    workflow.connect(inv_mask, 'out_file', upload_mask, 'in_files')
    
    # Apply the mask.
    apply_mask = pe.Node(fsl.ApplyMask(), name='apply_mask')
    workflow.connect(inv_mask, 'out_file', apply_mask, 'mask_file')
    workflow.connect(input_spec, 'image', apply_mask, 'in_file')
    
    # Make the ANTS template.
    avg_xf = _create_average_interface(**avg_opts)
    average = pe.Node(avg_xf, name='average')
    # Use the middle half of the images.
    offset = len(images) / 4
    average.inputs.images = sorted(images)[offset:len(images)-offset]

    # Register the images to create the warp and affine transformations.
    reg_xf = _create_registration_interface(**reg_opts)
    register = pe.Node(reg_xf, name='register')
    workflow.connect(apply_mask, 'out_file', register, 'moving_image')
    workflow.connect(average, 'output_average_image', register, 'fixed_image')
    
    # Apply the transforms to the input image.
    reslice = pe.Node(_create_reslice_interface(**rsmpl_opts), name='reslice')
    workflow.connect(input_spec, ('image', _gen_reslice_filename), reslice, 'output_image')
    workflow.connect(apply_mask, 'out_file', reslice, 'input_image')
    workflow.connect(average, 'output_average_image', reslice, 'reference_image')
    workflow.connect(register, 'forward_transforms', reslice, 'transforms')
    
    # Copy the DICOM meta-data.
    copy_meta = pe.Node(CopyMeta(), name='copy_meta')
    workflow.connect(input_spec, 'image', copy_meta, 'src_file')
    workflow.connect(reslice, 'output_image', copy_meta, 'dest_file')
    
    # Upload the resliced image to XNAT.
    upload_reg = pe.Node(XNATUpload(project=PROJECT, format='NIFTI'),
        name='upload_reg')
    upload_reg.inputs.subject = subject
    upload_reg.inputs.session = session
    upload_reg.inputs.reconstruction = recon
    workflow.connect(copy_meta, 'dest_file', upload_reg, 'in_files')
    
    return workflow

def _run_workflow(workflow):
    """
    Executes the given workflow.
    
    @param workflow: the workflow to run
    """
    # If debug is set, then diagram the workflow graph.
    if logger.level <= logging.DEBUG:
        fname = "%s.dot" % workflow.name
        if workflow.base_dir:
            grf = os.path.join(workflow.base_dir, fname)
        else:
            grf = fname
        workflow.write_graph(dotfilename=grf)
        logger.debug("The %s workflow graph is depicted at %s.png." % (workflow.name, grf))
    
    # Run the workflow.
    workflow.run()

def _generate_name(prefix):
    """
    @param: the name prefix
    @return: a unique name which starts with the given prefix
    """
    # The name suffix.
    suffix = file_helper.generate_file_name()
    
    return "%s_%s" % (prefix, suffix)

def _gen_reslice_filename(in_file):
    """
    @param in_file: the input scan image filename
    @return: the registered image filename
    """
    import re
    
    base, ext = re.match('(.*?)(\.[.\w]+)*$', in_file).groups()
    fname = "%s_%s" % (base, 'reg')
    if ext:
        return fname + ext
    else:
        return fname
    
def _gen_crop_op_string(cog):
    """
    @param cog: the center of gravity
    @return: the crop -roi option
    """
    return "-roi 0 -1 %d -1 0 -1 0 -1" % cog[1]
        
def _crop_posterior(image, cog):
    from nipype.interfaces import fsl
    
    crop_back = fsl.ImageMaths()
    crop_back.inputs.op_string = '-roi 0 -1 %d -1 0 -1 0 -1' % cog[1]
    crop_back.inputs.in_file = image
    return crop_back.run().outputs.out_file
    
def _create_mask_cluster_interface(**opts):
    """
    @param opts: the Nipype MRIVolCluster option overrides
    @return: a new MRIVolCluster interface
    """
    mask_opts = DEF_MASK_CLUSTER_OPTS.copy()
    mask_opts.update(opts)
    
    return MriVolCluster(**mask_opts)
    
def _create_average_interface(**opts):
    """
    @param opts: the Nipype ANTS AverageImages option overrides
    @return: a new ANTS average generation interface
    """
    avg_opts = DEF_ANTS_AVG_OPTS.copy()
    avg_opts.update(opts)
    
    return AverageImages(**avg_opts)

def _create_registration_interface(**opts):
    """
    @param opts: the Nipype ANTS Registration option overrides
    @return: a new ANTS Registration interface
    """
    reg_opts = DEF_ANTS_REG_OPTS.copy()
    reg_opts.update(opts)
    
    return Registration(**reg_opts)

def _create_reslice_interface(**opts):
    reslice_opts = DEF_ANTS_WARP_OPTS.copy()
    reslice_opts.update(opts)
    
    return ApplyTransforms(**reslice_opts)
