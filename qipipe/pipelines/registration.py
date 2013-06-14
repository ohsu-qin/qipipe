import os
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import AverageImages, ApplyTransforms
from nipype.interfaces.dcmstack import CopyMeta
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import DcmStack, MergeNifti, CopyMeta
from nipype.interfaces.utility import Select, IdentityInterface, Function
from ..helpers.xnat_helper import PROJECT
from ..interfaces import XNATDownload, XNATUpload, MriVolCluster
from ..helpers import xnat_helper, file_helper
from ..helpers.ast_config import read_config

import logging
logger = logging.getLogger(__name__)

DEF_CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'conf', 'registration.cfg')

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
    ``scans`` subdirectory of the ``base_dir`` specified in the options
    (default is the current directory).
    
    The average is taken on the middle half of the NiFTI scan images.
    These images are averaged into a fixed reference template image.

    The options include the Pyxnat Workflow initialization options, as well as
    the following options:

    - ``config``: an optional configuration file
    
    The configuration file can contain the following sections:

    - ``FSLMriVolCluster``: the FSL ``MriVolCluster`` interface options

    - ``ANTSAverage``: the ANTS ``Average`` interface options

    - ``ANTSRegistration``: the ANTS ``Registration`` interface options

    - ``ANTSApplyTransforms``: the ANTS ``ApplyTransforms`` interface options
    
    The default registration applies an affine followed by a symmetric normalization
    transform.
    
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

def _register(subject, session, dest, **opts):
    """
    Builds and runs the registration workflow.
    
    :param subject: the XNAT subject label
    :param session: the XNAT session label
    :param dest: the scan download directory
    :param opts: the workflow options
    :return: the warpd XNAT (subject, session, reconstruction) designator tuple
    """
    # Download the scan images.
    tgt = os.path.join(dest, subject, session)
    images = _download_scans(subject, session, tgt)
    
    # Make a unique registration reconstruction name. This permits more than one
    # registration to be stored for each input image without a name conflict.
    recon = _generate_name(REG_PREFIX)
    
    # Make the workflow
    workflow = create_workflow(subject, session, recon, images, **opts)
    # Execute the workflow
    workflow.run()
    
    # Return the recon specification.
    return (subject, session, recon)

def _download_scans(subject, session, dest):
    """
    Download the NIFTI scan files for the given session.
    
    :param subject: the XNAT subject label
    :param session: the XNAT session label
    :param dest: the destination directory path
    :return: the download file paths
    """

    with xnat_helper.connection() as xnat:
        return xnat.download(PROJECT, subject, session, dest=dest, container_type='scan', format='NIFTI')

def create_workflow(subject, session, recon, images, **opts):
    """
    Creates the nipype workflow for the given session images.
    
    :param subject: the XNAT subject label
    :param session: the XNAT session label
    :param recon: the XNAT registration reconstruction label
    :param images: the input session scan NiFTI stacks
    :param opts: the workflow options
    :return: the registration Workflow object
    """
    msg = 'Creating the %s %s registration workflow' % (subject, session)
    if opts:
        msg = msg + ' with options %s' % opts
    logger.debug("%s...", msg)

    # The configuration.
    cfg_file = opts.pop('config', None)
    if cfg_file:
        cfg = read_config(DEF_CONFIG_FILE, cfg_file)
    else:
        cfg = read_config(DEF_CONFIG_FILE)
    cfg_opts = dict(cfg)
    
    # The mask step options.
    mask_opts = cfg_opts.get('FSLMriVolCluster', {})
    # The average step options.
    avg_opts = cfg_opts.get('ANTSAverage', {})
    # The registration step options.
    reg_opts = cfg_opts.get('ANTSRegistration', {})
    # The reslice step options.
    reslice_opts = cfg_opts.get('ANTSApplyTransforms', {})

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
    
    # Upload the 4D image to XNAT.
    upload_4d = pe.Node(XNATUpload(project=PROJECT, reconstruction='4d', format='NIFTI'),
        name='upload_4d')
    upload_4d.inputs.subject = subject
    upload_4d.inputs.session = session
    workflow.connect(dce_merge, 'out_file', upload_4d, 'in_files')

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

    # Find large clusters of empty space on the cropped image.
    cluster_mask = pe.Node(MriVolCluster(**mask_opts), name='cluster_mask')
    workflow.connect(crop_back, 'out_file', cluster_mask, 'in_file')

    # Convert the cluster labels to a binary mask.
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
    
    # Make the ANTS template.
    average = pe.Node(AverageImages(**avg_opts), name='average')
    # Use the middle half of the images.
    offset = len(images) / 4
    average.inputs.images = sorted(images)[offset:len(images)-offset]

    # Register the images to create the warp and affine transformations.
    register = pe.Node(Registration(**reg_opts), name='register')
    workflow.connect(input_spec, 'image', register, 'moving_image')
    workflow.connect(average, 'output_average_image', register, 'fixed_image')
    workflow.connect(inv_mask, 'out_file', register, 'fixed_image_mask')
    workflow.connect(inv_mask, 'out_file', register, 'moving_image_mask')
    
    # Apply the transforms to the input image.
    reslice = pe.Node(ApplyTransforms(**reslice_opts), name='reslice')
    workflow.connect(input_spec, 'image', reslice, 'input_image')
    workflow.connect(input_spec, ('image', _gen_reslice_filename), reslice, 'output_image')
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
    
    :param workflow: the workflow to run
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
    :param: the name prefix
    :return: a unique name which starts with the given prefix
    """
    # The name suffix.
    suffix = file_helper.generate_file_name()
    
    return "%s_%s" % (prefix, suffix)

def _gen_reslice_filename(in_file):
    """
    :param in_file: the input scan image filename
    :return: the registered image filename
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
    :param cog: the center of gravity
    :return: the crop -roi option
    """
    return "-roi 0 -1 %d -1 0 -1 0 -1" % cog[1]
        
def _crop_posterior(image, cog):
    from nipype.interfaces import fsl
    
    crop_back = fsl.ImageMaths()
    crop_back.inputs.op_string = '-roi 0 -1 %d -1 0 -1 0 -1' % cog[1]
    crop_back.inputs.in_file = image
    return crop_back.run().outputs.out_file
