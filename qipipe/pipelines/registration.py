import nipype.pipeline.engine as pe
from nipype.interfaces.ants.registration import ANTS
from ..interfaces import XNATUpload

def create_registration_connections(input_node, input_field):
    """
    Creates the series stack connections.
    
    @param input_node: the input image files source node
    @param input_field: the input image files field name
    @return: the registration workflow connections
    """
    
    # The ANTS template node.
    tmpl_xf = create_template_interface()
    reg_template = pe.Node(tmpl_xf, name='reg_template')
    
    # The ANTS registration node.
    reg_xf = create_registration_interface()
    registration = pe.MapNode(reg_xf, name='registration', iterfields=['moving_image'])
    
    # Store the registration result in XNAT.
    store_reg = pe.Node(XNATUpload(project='QIN'), name='store_registration')


    # Test node.
    # TODO - enable template, disconnect moving and 'first' def
    moving = pe.Node(Glue(input_names=['in_files'], output_names=['rest'], function=rest), name='rest')

    
    return [
        (input_node, reg_template, [(input_field, 'in_files')]),
        # (input_node, registration, [(input_field, 'moving_image')]),
        (input_node, rest, [(input_field, 'in_files')]),
        (rest, registration, [('rest', 'moving_image')]),
        (reg_template, registration, [('template', 'fixed_image')]),
        (registration, store_reg, [('out_file', 'in_files')])]

def first(iterable):
    for item in iterable:
        return item

def create_template_interface():
    """
    @return: the ANTS template generation interface with default parameter settings
    """
    return Glue(input_names=['in_files'], output_names=['template'], function=first)

def rest(iterable):
    return list(iterable)[1:]

def create_registration_interface():
    """
    @return: a new ANTS interface with default parameter settings
    """
    ants = ANTS()
    ants.inputs.dimension = 3
    ants.inputs.output_transform_prefix = 'MY'
    ants.inputs.metric = ['CC']
    ants.inputs.metric_weight = [1.0]
    ants.inputs.radius = [5]
    ants.inputs.transformation_model = 'SyN'
    ants.inputs.gradient_step_length = 0.25
    ants.inputs.number_of_iterations = [50, 35, 15]
    ants.inputs.use_histogram_matching = True
    ants.inputs.mi_option = [32, 16000]
    ants.inputs.regularization = 'Gauss'
    ants.inputs.regularization_gradient_field_sigma = 3
    ants.inputs.regularization_deformation_field_sigma = 0
    ants.inputs.number_of_affine_iterations = [10000,10000,10000,10000,10000]
    
    return ants
    