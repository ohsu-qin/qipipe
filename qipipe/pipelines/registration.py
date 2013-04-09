import os
import nipype.interfaces.utility as util
import nipype.interfaces.ants as ants
import nipype.interfaces.io as io
import nipype.pipeline.engine as pe
from nipype.interfaces.ants import ANTS

ants = ANTS()
ants.inputs.dimension = 3
ants.inputs.output_transform_prefix = 'MY'
ants.inputs.metric = ['CC']
ants.inputs.fixed_image = ['T1.nii']
ants.inputs.moving_image = ['resting.nii']
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
