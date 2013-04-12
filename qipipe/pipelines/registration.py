import nipype.pipeline.engine as pe
from nipype.interfaces.utility import Function
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import WarpImageMultiTransform
from ..interfaces import XNATUpload

def run(*subject_dirs, **opts):
    """
    Builds and runs the registration workflow.
    
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: the workflow options
    @return: the new XNAT scans
    """
    # Make the registration workflow.
    wf = _create_workflow(*subject_dirs, **opts)
    
    # If debug is set, then diagram the registration workflow graph.
    if logger.level <= logging.DEBUG:
        grf = os.path.join(wf.base_dir, 'registration.dot')
        wf.write_graph(dotfilename=grf)
        logger.debug("The registration workflow graph is depicted at %s.png." % grf)
    
    # Run the registration workflow.
    wf.run()
    
    # Return the new XNAT sessions.
    xnat = XNAT()
    sess_specs = zip(wf.outputs.subject, stage.outputs.session)
    return [xnat.get_session('QIN', *spec) for spec in sess_specs]

def _create_workflow(input_node, **opts):
    """
    Creates the series stack connections.
    
    @param input_node: the input image files source node
    @param input_field: the input image files field name
    @return: the registration workflow
    """
    
    msg = 'Creating the registration workflow'
    if opts:
        msg = msg + ' with options %s...' % opts
    logger.debug("%s...", msg)
    wf = pe.Workflow(name='registration', **opts)
    
    # The registration connections.
    wf.connect(registration.create_registration_connections(self.collection, series_spec))
    
    # Set the top-level workflow input collection and subjects.
    wf.inputs.input_spec.collection = self.collection
    wf.inputs.input_spec.subjects = subjects
    
    # Make the ANTS template.
    tmpl_xf = create_template_interface()
    template = pe.Node(tmpl_xf, name='template')
    
    # Make the ANTS warp and affine transformations.
    reg_xf = create_transformations_interface()
    transforms = pe.Node(reg_xf, name='transforms')
    
    # Build the transformation series.
    xfmsif = Glue(input_fields=['warp', 'affine'], output_fields=['transforms'])
    xfm_series = pe.Node(xfmsif)
    
    # Resample the input images.
    resample = pe.Node(WarpImageMultiTransform(), name='resample')
    resample.inputs.input_image = 'structural.nii'
    resample.inputs.reference_image = 'ants_deformed.nii.gz'
    resample.inputs.transformation_series = ['ants_Warp.nii.gz','ants_Affine.txt']    
    
    # Store the registration result in XNAT.
    store_reg = pe.Node(XNATUpload(project='QIN'), name='store_registration')

    # TODO - finish template, remove moving, rest, first
    # The moving images.
    moving = pe.Node(Function(input_names=['in_files'], output_names=['moving_images'], function=rest), name='moving')

    wf.connect([
        (input_node, template, [(input_field, 'in_files')]),
        # (input_node, registration, [(input_field, 'moving_image')]),
        (input_node, moving, [(input_field, 'in_files')]),
        (moving, transforms, [('moving_images', 'moving_image')]),
        (template, transforms, [('template', 'fixed_image')]),
        (transforms, xfm_series, [('transforms', 'transform_series')]),
        (xfm_series, resample, [('transforms', 'transform_series')]),
        (registration, store_reg, [('out_file', 'in_files')])])
    
    return wf

def _first(iterable):
    for item in iterable:
        return item

def _create_template_interface():
    """
    @return: the ANTS template generation interface with default parameter settings
    """
    return Function(input_names=['in_files'], output_names=['template'], function=_first)

def _rest(iterable):
    return list(iterable)[1:]

def _create_registration_interface():
    """
    @return: a new ANTS Registration interface with the preferred parameter settings
    """
    
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
    