import os
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import AverageImages, WarpImageMultiTransform
from nipype.interfaces.dcmstack import CopyMeta
from ..interfaces import Glue, Copy, XNATDownload, XNATUpload
from ..helpers import xnat_helper
from ..helpers import file_helper

import logging
logger = logging.getLogger(__name__)


# AVG_FNAME = 'average'
# """The XNAT averaged reference image name."""
# 
# AVG_RECON = 'avg_1'
# """The XNAT averaging reconstruction name."""
# 
REG_PREFIX = 'reg'
"""The XNAT registration reconstruction name prefix."""

def run(*session_specs, **opts):
    """
    Registers the scan NiFTI images for the given sessions.
    
    Registration is split into two Nipype workflows, C{average} and C{register},
    as follows:
    
    The NiFTI scan images for each session are downloaded from XNAT into the
    C{scans} subdirectory of the C{base_dir} specified in the options (default is the current directory).
    
    The average workflow input is a directory containing the NiFTI scan images
    downloaded from XNAT. These images are averaged into a fixed reference
    template image. This template is stored in the same directory as the
    
    @param session_specs: the XNAT (subject, session) name tuples to register
    @param opts: the workflow options
    @return: the resampled XNAT (subject, session, reconstruction) designator tuples
    """

    # The work directory.
    work = opts.get('base_dir') or os.getcwd()
    # The scan image downloaad location.
    dest = os.path.join(work, 'scans')
    # Run the workflow on each session.
    recon_specs = [_register(sbj, sess, dest, **opts) for sbj, sess in session_specs]
    
    return recon_specs

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

def _register(subject, session, dest, **opts):
    """
    Builds and runs the registration workflow.
    
    @param subject: the XNAT subject label
    @param session: the XNAT session label
    @param dest: the scan download directory
    @param opts: the workflow options
    @return: the resampled XNAT (subject, session, reconstruction) designator tuple
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

def _create_workflow(subject, session, recon, images, **opts):
    """
    @param subject: the XNAT subject label
    @param session: the XNAT session label
    @param recon: the target XNAT reconstruction id
    @param opts: the pyxnat workflow creation options
    @return: the registration workflow
    """
    msg = 'Creating the registration workflow'
    if opts:
        msg = msg + ' with options %s' % opts
    logger.debug("%s...", msg)
    wf = pe.Workflow(name='register', **opts)
    
    # Make the ANTS template.
    avg_xf = _create_average_interface()
    average = pe.Node(avg_xf, name='average')
    average.inputs.images = images

    input_spec = pe.Node(IdentityInterface(fields=['subject', 'session', 'image']),
         name='input_spec')
    input_spec.inputs.subject = subject
    input_spec.inputs.session = session
    input_spec.iterables = dict(image=images).items()

    # Register the images to create the warp and affine transformations.
    reg_xf = _create_registration_interface()
    register = pe.Node(reg_xf, name='register')
    
    # Resample the input image.
    resample = pe.Node(WarpImageMultiTransform(),  name='resample')
    
    # Copy the DICOM meta-data.
    copy_meta = pe.Node(CopyMeta(), name='copy_meta')
    
    # Upload the resampled image to XNAT.
    upload = pe.Node(XNATUpload(project='QIN', reconstruction=recon, format='NIFTI'),
        name='upload')
    
    wf.connect([
        (input_spec, register, [('image', 'moving_image')]),
        (input_spec, resample, [('image', 'input_image')]),
        (input_spec, copy_meta, [('image', 'src_file')]),
        (input_spec, upload, [('subject', 'subject'), ('session', 'session')]),
        (average, register, [('output_average_image', 'fixed_image')]),
        (average, resample, [('output_average_image', 'reference_image')]),
        (register, resample, [('composite_transform', 'transformation_series')]),
        (resample, copy_meta, [('output_image', 'dest_file')]),
        (copy_meta, upload, [('dest_file', 'in_files')])])
    
    return wf

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
    
def _create_average_interface():
    """
    @return: the ANTS average generation interface with default parameter settings
    """
    avg = AverageImages()
    avg.inputs.dimension = 3
    avg.inputs.normalize = True
    
    return avg

def _create_registration_interface():
    """
    @return: a new ANTS Registration interface with the preferred parameter settings
    """
    # TODO - remove line below
    return Glue(input_names=['moving_image', 'fixed_image'], output_names=['composite_transform'])

    
    
    reg = Registration()
    reg.inputs.dimension = 2
    reg.inputs.output_transform_prefix = 'xfm'
    reg.inputs.transforms = ['Affine', 'SyN']
    reg.inputs.transform_parameters = [(2.0,), (0.25, 3.0, 0.0)]
    reg.inputs.number_of_iterations = [[15, 2], [10, 5, 3]] #[[1500, 200], [100, 50, 30]]
    reg.inputs.write_composite_transform = True
    reg.inputs.collapse_output_transforms = False
    reg.inputs.metric = ['Mattes']*2
    reg.inputs.metric_weight = [1]*2
    reg.inputs.radius_or_number_of_bins = [32]*2
    reg.inputs.sampling_strategy = ['Random', None]
    reg.inputs.sampling_percentage = [0.05, None]
    reg.inputs.convergence_threshold = [1.e-8, 1.e-9]
    reg.inputs.convergence_window_size = [20]*2
    reg.inputs.smoothing_sigmas = [[1,0], [2,1,0]]
    reg.inputs.shrink_factors = [[2,1], [3,2,1]]
    reg.inputs.use_estimate_learning_rate_once = [True, True]
    
    return reg
    