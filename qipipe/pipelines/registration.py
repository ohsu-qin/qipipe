import os
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import AverageImages, ApplyTransforms
from nipype.interfaces.dcmstack import CopyMeta
from ..interfaces import Glue, Copy, XNATDownload, XNATUpload
from ..helpers import xnat_helper
from ..helpers import file_helper

import logging
logger = logging.getLogger(__name__)


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
    smoothing_sigmas=[[1,0], [2,1,0]],
    shrink_factors=[[1,1], [3,2,1]],
    use_estimate_learning_rate_once=[True, True],
    output_transform_prefix='xfm',
    output_warped_image='warp.nii.gz',
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
    Registers the scan NiFTI images for the given sessions.
    
    Registration is split into two Nipype workflows, C{average} and C{register},
    as follows:
    
    The NiFTI scan images for each session are downloaded from XNAT into the
    C{scans} subdirectory of the C{base_dir} specified in the options (default is the current directory).
    
    The C{average} workflow input is a directory containing the NiFTI scan images
    downloaded from XNAT. These images are averaged into a fixed reference
    template image. This template is stored in the same directory as the

    The options include the Pyxnat Workflow initialization options, as well as
    the following key => dictionary options:
        - C{average}: the ANTS C{Average} interface options
        - C{register}: the ANTS C{AverageImages} interface options
        - C{warp}: the ANTS C{ApplyTransforms} interface options
    
    The registration applies an affine followed by a symmetric normalization transform.
    
    @param session_specs: the XNAT (subject, session) name tuples to register
    @param opts: the workflow options
    @return: the warpd XNAT (subject, session, reconstruction) designator tuples
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
    
    # The registration reconstruction name.
    recon = _generate_name(REG_PREFIX)
    # Make the workflow
    wf = _create_workflow(subject, session, recon, images, **opts)
    # Execute the workflow
    wf.run()
    
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
        return xnat.download('QIN', subject, session, dest=dest, container_type='scan', format='NIFTI')

def _create_workflow(subject, session, recon, images, **opts):
    """
    Creates the Pyxnat Workflow for the given specification.
    
    @param subject: the XNAT subject label
    @param session: the XNAT session label
    @param recon: the target XNAT reconstruction id
    @param opts: the workflow creation options described in L{registration.run}
    @return: the registration workflow
    """
    msg = 'Creating the registration workflow'
    if opts:
        msg = msg + ' with options %s' % opts
    logger.debug("%s...", msg)

    # The average step options.
    avg_opts = opts.pop('average', {})
    # The registration step options.
    reg_opts = opts.pop('register', {})
    # The warp step options.
    rsmpl_opts = opts.pop('warp', {})

    wf = pe.Workflow(name='register', **opts)

    # The workflow input (subject, session, image) tuples
    input_spec = pe.Node(IdentityInterface(fields=['subject', 'session', 'image']),
         name='input_spec')
    input_spec.inputs.subject = subject
    input_spec.inputs.session = session
    input_spec.iterables = dict(image=images).items()
    
    # Make the ANTS template.
    avg_xf = _create_average_interface(**avg_opts)
    average = pe.Node(avg_xf, name='average')
    average.inputs.images = images

    # Register the images to create the warp and affine transformations.
    reg_xf = _create_registration_interface(**reg_opts)
    register = pe.Node(reg_xf, name='register')
    
    # Apply the transforms to the input image.
    reslice = pe.Node(_create_reslice_interface(**rsmpl_opts), name='reslice')
    
    # Copy the DICOM meta-data.
    copy_meta = pe.Node(CopyMeta(), name='copy_meta')
    
    # Upload the resliced image to XNAT.
    upload = pe.Node(XNATUpload(project='QIN', reconstruction=recon, format='NIFTI'),
        name='upload')
    
    wf.connect([
        (input_spec, register, [('image', 'moving_image')]),
        (input_spec, reslice, [('image', 'input_image')]),
        (input_spec, copy_meta, [('image', 'src_file')]),
        (input_spec, upload, [('subject', 'subject'), ('session', 'session')]),
        (average, register, [('output_average_image', 'fixed_image')]),
        (average, reslice, [('output_average_image', 'reference_image')]),
        (register, reslice, [('forward_transforms', 'transforms')]),
        (reslice, copy_meta, [('output_image', 'dest_file')]),
        (copy_meta, upload, [('dest_file', 'in_files')])])
    
    return wf

def _run_workflow(wf):
    """
    Executes the given workflow.
    
    @param wf: the workflow to run
    """
    # If debug is set, then diagram the workflow graph.
    if logger.level <= logging.DEBUG:
        fname = "%s.dot" % wf.name
        if wf.base_dir:
            grf = os.path.join(wf.base_dir, fname)
        else:
            grf = fname
        wf.write_graph(dotfilename=grf)
        logger.debug("The %s workflow graph is depicted at %s.png." % (wf.name, grf))
    
    # Run the workflow.
    wf.run()

def _generate_name(prefix):
    """
    @param: the name prefix
    @return: a unique name which starts with the given prefix
    """
    # The name suffix.
    suffix = file_helper.generate_file_name()
    
    return "%s_%s" % (prefix, suffix)

def _first(in_files):
    for item in in_files:
        return item
    
def _create_average_interface(**opts):
    """
    @param opts: the Nipype ANTS AverageImages options
    @return: the ANTS average generation interface with default parameter settings
    """
    avg_opts = DEF_ANTS_AVG_OPTS.copy()
    avg_opts.update(opts)
    
    return AverageImages(**avg_opts)

def _create_registration_interface(**opts):
    """
    @param opts: the Nipype ANTS Registration options
    @return: a new ANTS Registration interface with the preferred parameter settings
    """
    reg_opts = DEF_ANTS_REG_OPTS.copy()
    reg_opts.update(opts)
    
    return Registration(**reg_opts)


    # reg = Registration()
    # reg.inputs.fixed_image =  [input_images[0], input_images[0] ]
    # reg.inputs.moving_image = [input_images[1], input_images[1] ]
    # reg.inputs.transforms = ['Affine', 'SyN']
    # reg.inputs.transform_parameters = [(2.0,), (0.25, 3.0, 0.0)]
    # reg.inputs.number_of_iterations = [[1500, 200], [100, 50, 30]]
    # reg.inputs.dimension = 3
    # reg.inputs.write_composite_transform = True
    # reg.inputs.metric = ['Mattes']*2
    # reg.inputs.metric_weight = [1]*2 # Default (value ignored currently by ANTs)
    # reg.inputs.radius_or_number_of_bins = [32]*2
    # reg.inputs.sampling_strategy = ['Random', None]
    # reg.inputs.sampling_percentage = [0.05, None]
    # reg.inputs.convergence_threshold = [1.e-8, 1.e-9]
    # reg.inputs.convergence_window_size = [20]*2
    # reg.inputs.smoothing_sigmas = [[1,0], [2,1,0]]
    # reg.inputs.shrink_factors = [[2,1], [3,2,1]]
    # reg.inputs.use_estimate_learning_rate_once = [True, True]
    # reg.inputs.use_histogram_matching = [True, True] # This is the default
    # reg.inputs.output_transform_prefix = 'thisTransform'
    # reg.inputs.output_warped_image = 'INTERNAL_WARPED.nii.gz'

    # antsRegistration --collapse-linear-transforms-to-fixed-image-header 0 --dimensionality 3 --interpolation Linear --output [ thisTransform, INTERNAL_WARPED.nii.gz ] --transform Affine[ 2.0 ] --metric Mattes[ /Users/loneyf/nipypeTestPath/01_T1_half.nii.gz, /Users/loneyf/nipypeTestPath/02_T1_half.nii.gz, 1, 32 ,Random,0.05 ] --convergence [ 15x2, 1e-08, 20 ] --smoothing-sigmas 1x0 --shrink-factors 2x1 --use-estimate-learning-rate-once 1 --use-histogram-matching 1 --transform SyN[ 0.25, 3.0, 0.0 ] --metric Mattes[ /Users/loneyf/nipypeTestPath/01_T1_half.nii.gz, /Users/loneyf/nipypeTestPath/02_T1_half.nii.gz, 1, 32  ] --convergence [ 10x5x3, 1e-09, 20 ] --smoothing-sigmas 2x1x0 --shrink-factors 3x2x1 --use-estimate-learning-rate-once 1 --use-histogram-matching 1 --winsorize-image-intensities [ 0.0, 1.0 ]  --write-composite-transform 1

    # antsRegistration --collapse-linear-transforms-to-fixed-image-header 0 --dimensionality 3 --interpolation Linear --output [ xfm, warped.nii.gz ] --transform Affine[ 2.0 ] --metric Mattes[ /Users/loneyf/workspace/qipipe/test/results/pipelines/registration/sarcoma/work/register/average/average.nii, /Users/loneyf/workspace/qipipe/test/results/pipelines/registration/sarcoma/work/scans/Sarcoma001/Sarcoma001_Session01/series009.nii.gz, 1, 32 ,Random,0.05 ] --convergence [ 15x2, 1e-08, 20 ] --smoothing-sigmas 1x0 --shrink-factors 2x1 --use-estimate-learning-rate-once 1 --use-histogram-matching 1 --transform SyN[ 0.25, 3.0, 0.0 ] --metric Mattes[ /Users/loneyf/workspace/qipipe/test/results/pipelines/registration/sarcoma/work/register/average/average.nii, /Users/loneyf/workspace/qipipe/test/results/pipelines/registration/sarcoma/work/scans/Sarcoma001/Sarcoma001_Session01/series009.nii.gz, 1, 32  ] --convergence [ 10x5x3, 1e-09, 20 ] --smoothing-sigmas 2x1x0 --shrink-factors 3x2x1 --use-estimate-learning-rate-once 1 --use-histogram-matching 1 --winsorize-image-intensities [ 0.0, 1.0 ]  --write-composite-transform 1

def _create_reslice_interface(**opts):
    reslice_opts = DEF_ANTS_WARP_OPTS.copy()
    reslice_opts.update(opts)
    
    return ApplyTransforms(**reslice_opts)
