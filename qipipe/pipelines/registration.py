import os
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import AverageImages, WarpImageMultiTransform
from ..interfaces import Glue, XNATDownload, XNATUpload
from ..helpers import xnat_helper

import logging
logger = logging.getLogger(__name__)

RECON_NAME = 'reg_1'
"""The XNAT reconstruction label."""

def run(*session_specs, **opts):
    """
    Builds and runs the registration workflow.
    
    @param session_specs: the XNAT (subject, session) label tuples to register
    @param opts: the workflow options
    @return: the resampled XNAT (subject, session, reconstruction) designator tuples
    """
    
    # Make the registration workflow.
    wf = _create_workflow(*session_specs, **opts)

    # If debug is set, then diagram the registration workflow graph.
    if logger.level <= logging.DEBUG:
        if wf.base_dir:
            grf = os.path.join(wf.base_dir, 'staging.dot')
        else:
            grf = 'staging.dot'
        wf.write_graph(dotfilename=grf)
        logger.debug("The registration workflow graph is depicted at %s.png." % grf)
    
    # Run the registration workflow.
    with xnat_helper.connection():
        wf.run()
    
    # Return the new XNAT reconstruction specs.
    return [(sbj, sess, RECON_NAME) for sbj, sess in session_specs]

def _create_workflow(*session_specs, **opts):
    """
    @param session_specs: the input XNAT (subject, session) label tuples
    @param opts: the pyxnat workflow creation options
    @return: the registration workflow
    """
    
    msg = 'Creating the registration workflow'
    if opts:
        msg = msg + ' with options %s' % opts
    logger.debug("%s...", msg)
    wf = pe.Workflow(name='registration', **opts)
    
    # The (session, subject) label inputs.
    session_spec = pe.Node(IdentityInterface(fields=['subject', 'session']),
        name='session_spec')
    # Iterate over each session.
    subjects = [spec[0] for spec in session_specs]
    sessions = [spec[1] for spec in session_specs]
    session_spec.iterables = dict(subject=subjects, session=sessions).items()
    
    # Download the scan NIFTI files.
    download = pe.Node(XNATDownload(project='QIN', container_type='scan', format='NIFTI'),
        name='download')
    
    # Make the ANTS template.
    avg_xf = _create_average_interface()
    average = pe.Node(avg_xf, name='average')
    
    # Register the images to create the warp and affine transformations.
    reg_xf = _create_registration_interface()
    registration = pe.MapNode(reg_xf, iterfield='moving_image', name='registration')
    
    # Resample the input images.
    resample = pe.MapNode(WarpImageMultiTransform(), iterfield=['input_image', 'transformation_series'], name='resample')
    
    # Upload the resampled images to XNAT.
    upload = pe.Node(XNATUpload(project='QIN', reconstruction=RECON_NAME, format='NIFTI'),
        name='upload')
    
    wf.connect([
        (session_spec, download, [('subject', 'subject'), ('session', 'session')]),
        (session_spec, upload, [('subject', 'subject'), ('session', 'session')]),
        (download, average, [('out_files', 'images')]),
        (download, registration, [('out_files', 'moving_image')]),
        (download, resample, [('out_files', 'input_image')]),
        (average, registration, [('output_average_image', 'fixed_image')]),
        (average, resample, [('output_average_image', 'reference_image')]),
        (registration, resample, [('composite_transform', 'transformation_series')]),
        (resample, upload, [('output_image', 'in_files')])])
    
    return wf
 
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
    