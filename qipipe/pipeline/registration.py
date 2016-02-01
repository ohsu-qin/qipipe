import os
import re
import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function, Merge)
from nipype.interfaces.ants import (AverageImages, Registration,
                                    ApplyTransforms)
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import CopyMeta
import qiutil
from qiutil.logging import logger
from ..interfaces import (Copy, XNATUpload)
from ..interfaces.ants import AffineInitializer
from ..helpers import bolus_arrival
from .workflow_base import WorkflowBase
from .pipeline_error import PipelineError

REG_PREFIX = 'reg_'
"""The XNAT registration resource name prefix."""

REG_CONF_FILE = 'registration.cfg'
"""The registration workflow configuration."""

ANTS_CONF_SECTIONS = ['ants.Registration']
"""The common ANTs registration configuration sections."""

ANTS_INITIALIZER_CONF_SECTION = 'ants.AffineInitializer'
"""The initializer ANTs registration configuration sections."""

FNIRT_CONF_SECTIONS = ['fsl.FLIRT', 'fsl.FNIRT']
"""The FNIRT registration configuration sections."""


def run(technique, project, subject, session, scan, ref_0, *images, **opts):
    """
    Runs the registration workflow on the given session scan images.
    
    :param technique: the :attr:`RegistrationWorkflow.technique`
    :param project: the project name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param ref_0: the initial fixed reference image
    :param images: the image files to register
    :param opts: the :class:`RegistrationWorkflow` initializer
        and :meth:`RegistrationWorkflow.run` options
    :return: the realigned image file path array
    """
    # Extract the run options.
    run_opts = {k: opts.pop(k) for k in ['dest', 'mask'] if k in opts}
    # Make the workflow.
    reg_wf = RegistrationWorkflow(project=project, technique=technique,
                                  **opts)
    # Run the workflow.
    return reg_wf.run(subject, session, scan, ref_0, *images, **run_opts)


def generate_resource_name():
    """
    Makes a unique registration resource name. Uniqueness permits more
    than one registration to be stored for a given session without a
    name conflict.
    
    :return: a unique XNAT registration resource name
    """
    return REG_PREFIX + qiutil.file.generate_file_name()


class RegistrationWorkflow(WorkflowBase):
    """
    The RegistrationWorkflow class builds and executes the registration workflow.
    The workflow registers an input NIfTI scan image against the input reference
    image and uploads the realigned image to XNAT.
    
    The registration workflow input is the *input_spec* node consisting of the
    following input fields:
    
    - *subject*: the subject name
    
    - *session*: the session name
    
    - *scan*: the scan number
    
    - *mask*: the mask to apply to the images
    
    - *initial_transform*: the starting affine transform to apply
    
    - *reference*: the fixed reference image
    
    - *image*: the image file to register
    
    The mask can be obtained by running the
    :class:`qipipe.pipeline.mask.MaskWorkflow` workflow.
    
    Three registration techniques are supported:
    
    - ``ants``: ANTS_ SyN_ symmetric normalization diffeomorphic registration
      (default)
    
    - ``fnirt``: FSL_ FNIRT_ non-linear registration
    
    - ``mock``: Test technique which copies each input scan image to the
      output image file
    
    The optional workflow configuration file can contain overrides for the
    Nipype interface inputs in the following sections:
    
    - ``AffineInitializer``: the :class:`qipipe.interfaces.ants.utils.AffineInitializer`
       options
    
    - ``ants.Registration``: the ANTs `Registration interface`_ options
    
    - ``ants.ApplyTransforms``: the ANTs `ApplyTransform interface`_ options
    
    - ``fsl.FNIRT``: the FSL `FNIRT interface`_ options
    
    :Note: Since the XNAT *resource* name is unique, a
        :class:`qipipe.pipeline.registration.RegistrationWorkflow`
        instance can be used for only one registration workflow.
        Different registration inputs require different
        :class:`qipipe.pipeline.registration.RegistrationWorkflow`
        instances.
    
    .. _ANTS: http://stnava.github.io/ANTs/
    .. _ApplyTransform interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.resampling.html
    .. _FNIRT: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FNIRT#Research_Overview
    .. _FNIRT interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.fsl.preprocess.html
    .. _FSL: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FSL
    .. _Registration interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.registration.html
    .. _SyN: http://www.ncbi.nlm.nih.gov/pubmed/17659998
    """
    
    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.
        
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer options, as well as the following keyword arguments:
        :keyword technique: the required registration :attr:`technique`
        :keyword resource: the XNAT resource name to use (default is
            an auto-generated name beginning with ``reg_``:attr:`technique`_)
        :keyword initialize: flag indicating whether to create an initial
            affine transform (ANTs only, default false)
        :keyword recursive: flag indicating whether to perform step-wise
            iterative recursive registration
        """
        super(RegistrationWorkflow, self).__init__(logger=logger(__name__),
                                                   **opts)
        
        technique_opt = opts.pop('technique', None)
        if not technique_opt:
            raise PipelineError('The registration technique was not specified.')
        self.technique = technique_opt.lower()
        """
        The lower-case XNAT registration technique. The built-in techniques
        include ``ants``, `fnirt`` and ``mock``.
        """
        
        rsc_opt = opts.pop('resource', None)
        self.resource = rsc_opt or generate_resource_name()
        """The XNAT resource name used for all runs against this
        workflow instance."""
        
        self.workflow = self._create_realignment_workflow(**opts)
        """The registration realignment workflow."""
    
    def run(self, subject, session, scan, ref_0, *images, **opts):
        """
        Runs the registration workflow on the given session scan images.
        
        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param ref_0: the required bolus uptake volume index
        :param images: the input session scan images
        :param opts: the following keyword arguments:
        :option mask: the image mask file path
        :option dest: the realigned image target directory (default is the
            current directory)
        :return: the realigned output file paths
        """
        if not images:
            return []
        # Sort the images by volume number.
        sorted_scans = sorted(images)
        
        # The target location.
        dest = opts.get('dest')
        if dest:
            dest = os.path.abspath(dest)
        else:
            dest = os.getcwd()
        
        # The recursive flag.
        recursive = opts.get('recursive', False)
        # The execution workflow.
        exec_wf = self._create_execution_workflow(ref_0, dest, recursive)
        
        # Set the execution workflow inputs.
        input_spec = exec_wf.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        mask = opts.get('mask')
        if mask:
            input_spec.inputs.mask = mask
        
        # Iterate over the registration inputs.
        iter_reg_input = exec_wf.get_node('iter_reg_input')
        iter_reg_input.iterables = ('image', images)
        
        # Execute the workflow.
        self.logger.debug("Executing the %s workflow on %s %s..." %
                         (self.workflow.name, subject, session))
        self._run_workflow(exec_wf)
        self.logger.debug("Executed the %s workflow on %s %s." %
                         (self.workflow.name, subject, session))
        
        # Return the output files.
        return [os.path.join(dest, filename(image)) for image in images]
    
    def _create_execution_workflow(self, ref_0, dest, recursive=False):
        """
        Makes the registration execution workflow on the given session
        scan images.
        
        The execution workflow input is the *input_spec* node consisting of
        the following input fields:
        
        - *subject*: the subject name
        
        - *session*: the session name
        
        - *scan*: the scan number
        
        - *mask*: the mask to apply to the images
        
        - *reference*: the fixed reference for the given image registration
        
        In addition, the caller has the responsibility of setting the
        ``iter_reg_input`` iterables to the 3D image files to realign.
        
        If the *recurse* option is set, then the *reference* input is set
        by :meth:`recurse`. Otherwise, *ref_0* is used for each registration.
        
        :param ref_0: the initial fixed reference image
        :param dest: the target realigned image directory
        :param recursive: whether to use each realigned imaged as the
            succeeding registration reference
        :return: the execution workflow
        """
        if not ref_0:
            raise PipelineError('Registration workflow is missing the initial' +
                                ' fixed reference image')
        if not dest:
            raise PipelineError('Registration workflow is missing the' +
                             ' destination directory')
        self.logger.debug("Creating the registration execution workflow"
                           " with initial reference %s..." % ref_0)
        
        # The execution workflow.
        exec_wf = pe.Workflow(name='reg_exec', base_dir=self.workflow.base_dir)
        
        # The registration workflow input.
        input_fields = ['subject', 'session', 'scan', 'mask', 'ref_0',
                        'resource']
        input_spec = pe.Node(IdentityInterface(fields=input_fields),
                             name='input_spec')
        # The image hierarchy.
        exec_wf.connect(input_spec, 'subject',
                        self.workflow, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session',
                        self.workflow, 'input_spec.session')
        exec_wf.connect(input_spec, 'scan',
                        self.workflow, 'input_spec.scan')
        # The registration mask.
        exec_wf.connect(input_spec, 'mask',
                        self.workflow, 'input_spec.mask')
        # The initial fixed reference image.
        input_spec.inputs.ref_0 = ref_0
        # The registration resource name.
        input_spec.inputs.resource = self.resource
        
        # The realignment child workflow iterator.
        iter_reg_fields = ['image', 'reference']
        iter_reg_input = pe.Node(IdentityInterface(fields=iter_reg_fields),
                                 name='iter_reg_input')
        exec_wf.connect(iter_reg_input, 'image',
                        self.workflow, 'input_spec.moving_image')
        exec_wf.connect(iter_reg_input, 'reference',
                        self.workflow, 'input_spec.reference')
        
        # If the recursive flag is set, then set the recursive
        # realigned -> reference connections. Otherwise, the fixed
        # reference is always the initial ref_0 image.
        if recursive:
            exec_wf.connect_iterables(iter_reg_input, copy_realigned,
                                      recurse, ref_0=ref_0)
        else:
            iter_reg_input.inputs.reference = ref_0
        
        # The output destination directory.
        if not os.path.exists(dest):
            os.makedirs(dest)
        
        # Collect the realigned images.
        collect_realigned = pe.JoinNode(IdentityInterface(fields=['images']),
                                        joinsource='iter_reg_input',
                                        joinfield='images',
                                        name='collect_realigned')
        exec_wf.connect(self.workflow, 'output_spec.out_file',
                        collect_realigned, 'images')
        
        # Make the profile.
        cr_prf_fields = ['technique', 'configuration', 'sections', 'dest']
        cr_prf_xfc = Function(input_names=cr_prf_fields,
                              output_names=['out_file'],
                              function=create_profile)
        cr_prf = pe.Node(cr_prf_xfc, name='create_profile')
        cr_prf.inputs.technique = self.technique
        cr_prf.inputs.configuration = self.configuration
        cr_prf.inputs.sections = self.profile_sections
        cr_prf.inputs.dest = REG_CONF_FILE
        
        # Merge the profile and registration result into one list.
        concat_uploads = pe.Node(Merge(2), name='concat_uploads')
        exec_wf.connect(cr_prf, 'out_file', concat_uploads, 'in1')
        exec_wf.connect(collect_realigned, 'images', concat_uploads, 'in2')
        
        # Upload the profile and registration result into the XNAT
        # registration resource.
        upload_reg_xfc = XNATUpload(project=self.project,
                                    modality='MR')
        upload_reg = pe.Node(upload_reg_xfc, name='upload_reg')
        exec_wf.connect(input_spec, 'subject', upload_reg, 'subject')
        exec_wf.connect(input_spec, 'session', upload_reg, 'session')
        exec_wf.connect(input_spec, 'scan', upload_reg, 'scan')
        exec_wf.connect(input_spec, 'resource', upload_reg, 'resource')
        exec_wf.connect(concat_uploads, 'out', upload_reg, 'in_files')
        
        # Copy the realigned images to the destination directory.
        #
        # FIXME: copying the realigned images individually with a Copy
        # submits a separate SGE job for each copy, contrary to the
        # defaults.cfg Copy setting. Although the defaults.cfg is
        # read into the registration workflow config, the defaults
        # are not applied, as they are for the parent qipipeline
        # workflow and its directly wired child workflows. The
        # registration workflow is built indirectly via the master
        # qipipeline register function for each scan input.
        # The work-around is to copy all of the realigned files
        # without a SGE submit in the copy_files function.
        # TODO: why doesn't registration recognize the default.cfg
        # Copy run_without_submitting setting?
        copy_realigned_xfc = Function(input_names=['in_files', 'dest'],
                                      output_names=['out_files'],
                                      function=copy_files)
        copy_realigned = pe.Node(copy_realigned_xfc, name='copy_realigned')
        copy_realigned.inputs.dest = dest
        exec_wf.connect(collect_realigned, 'images', copy_realigned, 'in_files')
        
        # The execution output.
        output_spec = pe.Node(IdentityInterface(fields=['images']),
                              name='output_spec')
        exec_wf.connect(collect_realigned, 'images', output_spec, 'images')
        
        self.logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(exec_wf)
        
        return exec_wf
    
    def _create_realignment_workflow(self, **opts):
        """
        Creates the workflow which registers and resamples images.
        The registration workflow performs the following steps:
        
        - Generates a unique XNAT resource name
        
        - Set the mask and realign workflow inputs
        
        - Run these workflows
        
        - Upload the realign outputs to XNAT
        
        :param opts: the following workflow options:
        :keyword base_dir: the workflow directory
            (default a new temp directory)
        :keyword initialize: flag indicating whether to create an
            initial affine transform (ANTs only, default false)
        :return: the Workflow object
        """
        self.logger.debug('Creating the registration realignment workflow...')
        
        # The workflow.
        base_dir = opts.get('base_dir', tempfile.mkdtemp(prefix='qipipe_'))
        realign_wf = pe.Workflow(name='registration', base_dir=self.base_dir)
        
        # The workflow input.
        in_fields = ['subject', 'session', 'scan', 'moving_image', 'reference',
                     'mask', 'resource']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        input_spec.inputs.resource = self.resource
        
        # Copy the DICOM meta-data. The copy target is set by the technique
        # node defined below.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        realign_wf.connect(input_spec, 'moving_image', copy_meta, 'src_file')
        
        # The input file name without directory.
        input_filename_xfc = Function(input_names=['in_file'],
                                      output_names=['out_file'],
                                      function=filename)
        input_filename = pe.Node(input_filename_xfc, name='input_filename')
        realign_wf.connect(input_spec, 'moving_image',
                           input_filename, 'in_file')
        
        if self.technique == 'ants':
            self.profile_sections = ANTS_CONF_SECTIONS
            # Nipype bug work-around:
            # Setting the registration metric and metric_weight inputs
            # after the node is created results in a Nipype input trait
            # dependency warning. Avoid this warning by setting these
            # inputs in the constructor from the values in the configuration.
            reg_cfg = self._interface_configuration(Registration)
            metric_inputs = {field: reg_cfg[field]
                             for field in ['metric', 'metric_weight']
                             if field in reg_cfg}
            # Register the images to create the rigid, affine and SyN
            # ANTS transformations. The float option is set to reduce
            # the output image size by app. 4x.
            reg_xfc = Registration(float=True, **metric_inputs)
            register = pe.Node(reg_xfc, name='register')
            realign_wf.connect(input_spec, 'reference',
                               register, 'fixed_image')
            realign_wf.connect(input_spec, 'moving_image',
                               register, 'moving_image')
            realign_wf.connect(input_spec, 'mask',
                               register, 'fixed_image_mask')
            realign_wf.connect(input_spec, 'mask',
                               register, 'moving_image_mask')
            
            # If the initialize option is set, then make an initial
            # transform.
            initialize = opts.get('initialize')
            if initialize:
                self.profile_sections.append(ANTS_INITIALIZER_CONF_SECTION)
                init_xfm = pe.Node(AffineInitializer(), name='initialize_affine')
                realign_wf.connect(input_spec, 'reference',
                                   init_xfm, 'fixed_image')
                realign_wf.connect(input_spec, 'moving_image',
                                   init_xfm, 'moving_image')
                realign_wf.connect(input_spec, 'mask',
                                   init_xfm, 'image_mask')
                realign_wf.connect(init_xfm, 'affine_transform',
                                   register, 'initial_moving_transform')
                # Work around the following Nipype bug:
                # * If the Registration has an initial_moving_transform,
                #   then the default invert_initial_moving_transform value
                #   is not applied, resulting in the following error:
                #
                #   TraitError: Each element of the 'forward_invert_flags' trait
                #   of a RegistrationOutputSpec instance must be a boolean, but a
                #   value of <undefined> <class 'traits.trait_base._Undefined'>
                #   was specified.
                #
                #   The forward_invert_flags output field is set from the
                #   invert_initial_moving_transform input field. Even though
                #   the invert_initial_moving_transform trait specifies
                #   default=False, the invert_initial_moving_transform value
                #   is apparently undefined. Perhaps the input trait should
                #   also set the usedefault option. The work-around is to
                #   always set the the invert_initial_moving_transform field.
                register.inputs.invert_initial_moving_transform = False
            # Apply the transforms to the input image.
            apply_xfm = pe.Node(ApplyTransforms(), name='apply_xfm')
            realign_wf.connect(input_spec, 'reference',
                               apply_xfm, 'reference_image')
            realign_wf.connect(input_spec, 'moving_image',
                               apply_xfm, 'input_image')
            realign_wf.connect(input_filename, 'out_file',
                               apply_xfm, 'output_image')
            realign_wf.connect(register, 'forward_transforms',
                               apply_xfm, 'transforms')
            # Copy the meta-data.
            realign_wf.connect(apply_xfm, 'output_image',
                               copy_meta, 'dest_file')
        elif self.technique == 'fnirt':
            self.profile_sections = FNIRT_CONF_SECTIONS
            # Make the affine transformation.
            flirt = pe.Node(fsl.FLIRT(), name='flirt')
            realign_wf.connect(input_spec, 'reference', flirt, 'reference')
            realign_wf.connect(input_spec, 'moving_image', flirt, 'in_file')
            # Copy the input to a work directory, since FNIRT adds
            # temporary files to the input image location.
            fnirt_copy_moving = pe.Node(Copy(), name='fnirt_copy_moving')
            realign_wf.connect(input_spec, 'moving_image',
                               fnirt_copy_moving, 'in_file')
            # Register the image.
            fnirt = pe.Node(fsl.FNIRT(), name='fnirt')
            realign_wf.connect(input_spec, 'reference', fnirt, 'ref_file')
            realign_wf.connect(flirt, 'out_matrix_file', fnirt, 'affine_file')
            realign_wf.connect(fnirt_copy_moving, 'out_file', fnirt, 'in_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'inmask_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'refmask_file')
            realign_wf.connect(input_filename, 'out_file', fnirt, 'warped_file')
            # Copy the meta-data.
            realign_wf.connect(fnirt, 'warped_file', copy_meta, 'dest_file')
        elif self.technique == 'mock':
            self.profile_sections = []
            # Copy the input scan file to an output file.
            mock_copy = pe.Node(Copy(), name='mock_copy')
            realign_wf.connect(input_spec, 'moving_image',
                               mock_copy, 'in_file')
            realign_wf.connect(mock_copy, 'out_file', copy_meta, 'dest_file')
        else:
            raise PipelineError("Registration technique not recognized: %s" %
                                self.technique)
        
        # The output is the realigned image.
        output_spec = pe.Node(IdentityInterface(fields=['out_file']),
                              name='output_spec')
        realign_wf.connect(copy_meta, 'dest_file', output_spec, 'out_file')
        
        self._configure_nodes(realign_wf)
        
        self.logger.debug("Created the %s workflow." % realign_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(realign_wf)
        
        return realign_wf


### Utility functions called by the workflow nodes. ###

def create_profile(technique, configuration, sections, dest):
    """
    :meth:`qipipe.helpers.metadata.create_profile` wrapper.
    
    :param technique: the modeling technique
    :param configuration: the modeling workflow interface settings
    :param sections: the profile sections
    :param dest: the output profile file path
    """
    
    from qipipe.helpers import metadata
    
    # The correct technique names.
    TECHNIQUE_NAMES = dict(ants='ANTs', fnirt='FNIRT', mock='Mock')
    
    technique = TECHNIQUE_NAMES.get(technique, technique)
    
    return metadata.create_profile(configuration, sections, dest=dest,
                                   general=dict(technique=technique))


def filename(in_file):
    """
    :param in_file: the input file path
    :return: the file name without a directory
    """
    import os
    
    return os.path.split(in_file)[1]


def copy_files(in_files, dest):
    """
    :param in_files: the input files
    :param dest: the destination directory
    :return: the output files
    """
    from qipipe.interfaces import Copy
    
    return [Copy(in_file=in_file, dest=dest).run().outputs.out_file
            for in_file in in_files]


def recurse(workflow, input_nodes, output_nodes, ref_0):
    """
    Connects the reference input for the given reference workflow input
    nodes. The reference for the first input node is the *ref_0* file.
    The reference for each subsequent node is the prior registration
    result.
    
    :param workflow: the workflow delegate which connects nodes
    :param input_nodes: the iterable expansion input scan image nodes
    :param output_nodes: the iterable expansion registration output
        nodes
    :param ref_0: the starting reference input node
    """
    # The reference for the first node is the initial reference.
    input_nodes[0].inputs.reference = ref_0
    # The reference for the remaining nodes is the previous registration
    # result.
    for i in range(1, node_cnt - 1):
        workflow.connect(output_nodes[i], 'out_file',
                         input_nodes[i + 1], 'reference')
