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
from ..helpers import bolus_arrival
from .workflow_base import WorkflowBase
from .pipeline_error import PipelineError

REG_PREFIX = 'reg'
"""The XNAT registration resource name prefix."""


def run(project, subject, session, scan, bolus_arrival_index, *images, **opts):
    """
    Runs the registration workflow on the given session scan images.

    :param project: the project name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param bolus_arrival_index: the required bolus uptake volume index
    :param images: the input session scan images
    :param opts: the :class:`RegistrationWorkflow` initializer
        and :meth:`RegistrationWorkflow.run` options
    :return: the realigned image file path array
    """
    # Extract the run options.
    run_opts = {k: opts.pop(k) for k in ['dest', 'mask'] if k in opts}
    # Make the workflow.
    reg_wf = RegistrationWorkflow(project, **opts)
    # Run the workflow.
    return reg_wf.run(subject, session, scan, bolus_arrival_index, *images,
                      **run_opts)


def generate_resource_name():
    """
    Makes a unique registration resource name. Uniqueness permits more than
    one registration to be stored for a given session without a name conflict.

    :return: a unique XNAT registration resource name
    """
    return "%s_%s" % (REG_PREFIX, qiutil.file.generate_file_name())


class RegistrationWorkflow(WorkflowBase):
    """
    The RegistrationWorkflow class builds and executes the registration workflow.
    The workflow registers an input NiFTI scan image against the input reference
    image and uploads the realigned image to XNAT.

    The registration workflow input is the *input_spec* node consisting of the
    following input fields:

    - *subject*: the subject name

    - *session*: the session name

    - *scan*: the scan number

    - *mask*: the mask to apply to the images

    - *reference*: the fixed reference image

    - *image*: the image file to register

    The mask can be obtained by running the
    :class:`qipipe.pipeline.mask.MaskWorkflow` workflow.

    The reference can be obtained by running the
    :class:`qipipe.pipeline.reference.ReferenceWorkflow` workflow.

    The output realigned image file is named the same as the input scan
    image file name is in the XNAT registration resource. The default
    resource name is auto-generated.
    
    Three registration techniques are supported:

    - ``ants``: ANTS_ SyN_ symmetric normalization diffeomorphic registration
      (default)

    - ``fnirt``: FSL_ FNIRT_ non-linear registration
    
    - ``mock``: Test technique which copies each input scan image to the
      output image file

    The optional workflow configuration file can contain overrides for the
    Nipype interface inputs in the following sections:

    - ``ants.Registration``: the ANTS `Registration interface`_ options

    - ``ants.ApplyTransforms``: the ANTS `ApplyTransform interface`_ options

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

    def __init__(self, project, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param project: the XNAT project name
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer options, as well as the following options:
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword resource: the XNAT resource name to use
        :keyword technique: the case-insensitive workflow technique
            (``ANTS`` or ``FNIRT``, default ``ANTS``)
        """
        super(RegistrationWorkflow, self).__init__(project, logger(__name__),
                                                   **opts)

        rsc = opts.pop('resource', None)
        if not rsc:
            rsc = generate_resource_name()
        self.resource = rsc
        """The XNAT resource name used for all runs against this workflow
        instance."""

        self.workflow = self._create_realignment_workflow(**opts)
        """The registration realignment workflow."""

    def run(self, subject, session, scan, bolus_arrival_index, *images, **opts):
        """
        Runs the registration workflow on the given session scan images.

        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param bolus_arrival_index: the required bolus uptake volume index
        :param images: the input session scan images
        :param opts: the following options:
        :option mask: the image mask file path
        :option dest: the realigned image target directory (default is the
            current directory)
        :return: the realigned output file paths
        """
        if not images:
            return []
        # Sort the images by volume number.
        sorted_scans = sorted(images)

        # Split the images into pre- and post-arrival.
        pre_arrival = sorted_scans[0: bolus_arrival_index + 1]
        reg_input = sorted_scans[bolus_arrival_index + 1:]
        
        # The initial reference image occurs at bolus arrival.
        ref_0_image = pre_arrival[bolus_arrival_index]

        # The target location.
        dest = opts.get('dest', None)
        if dest:
            dest = os.path.abspath(dest)
        else:
            dest = os.getcwd()

        # The execution workflow.
        exec_wf = self._create_execution_workflow(bolus_arrival_index,
                                                  ref_0_image, dest)

        # Set the execution workflow inputs.
        input_spec = exec_wf.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.pre_arrival = pre_arrival
        mask = opts.get('mask', None)
        if mask:
            input_spec.inputs.mask = mask

        # Iterate over the registration inputs.
        iter_reg_input = exec_wf.get_node('iter_reg_input')
        iter_reg_input.iterables = ('image', reg_input)

        # Execute the workflow.
        self._logger.debug("Executing the %s workflow on %s %s..." %
                         (self.workflow.name, subject, session))
        self._run_workflow(exec_wf)
        self._logger.debug("Executed the %s workflow on %s %s." %
                         (self.workflow.name, subject, session))

        # Return the output files.
        return [os.path.join(dest, filename(image)) for image in images]

    def _create_execution_workflow(self, bolus_arrival_index, ref_0_image,
                                   dest):
        """
        Makes the registration execution workflow on the given session
        scan images.

        The execution workflow input is the *input_spec* node consisting of
        the following input fields:

        - *subject*: the subject name

        - *session*: the session name

        - *scan*: the scan number

        - *mask*: the mask to apply to the images

        - *pre_arrival*: the scan images prior to bolus arrival, which will
          not be realigned.

        - *reference*: the fixed reference for the given image registration
        
        In addition, the caller has the responsibility of setting the
        ``iter_reg_input`` iterables to the 3D image files to realign.

        The *reference* input is set by :meth:`connect_reference`.

        :param bolus_arrival_index: the required bolus uptake volume index
        :param ref_0_image: the required initial fixed reference image
        :param dest: the required target realigned image directory
        :return: the execution workflow
        """
        if bolus_arrival_index == None:
            raise PipelineError('Registration workflow is missing the bolus' +
                             ' arrival index')
        if not ref_0_image:
            raise PipelineError('Registration workflow is missing the initial' +
                             ' fixed reference image')
        if not dest:
            raise PipelineError('Registration workflow is missing the' +
                             ' destination directory')
        self._logger.debug("Creating the registration execution workflow"
                           " with bolus arrival index %d and initial reference"
                           " %s..." % (bolus_arrival_index, ref_0_image))

        # The execution workflow.
        exec_wf = pe.Workflow(name='reg_exec', base_dir=self.workflow.base_dir)

        # The registration workflow input.
        input_fields = ['subject', 'session', 'scan', 'pre_arrival', 'mask', 'ref_0',
                        'resource']
        input_spec = pe.Node(IdentityInterface(fields=input_fields),
                             name='input_spec')
        input_spec.inputs.ref_0 = ref_0_image
        exec_wf.connect(input_spec, 'subject',
                        self.workflow, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session',
                        self.workflow, 'input_spec.session')
        exec_wf.connect(input_spec, 'scan',
                        self.workflow, 'input_spec.scan')
        exec_wf.connect(input_spec, 'mask',
                        self.workflow, 'input_spec.mask')
        input_spec.inputs.resource = self.resource

        # The input images are iterable. The reference is set by the
        # connect_reference method below.
        iter_reg_input = pe.Node(IdentityInterface(fields=['image', 'reference']),
                             name='iter_reg_input')
        exec_wf.connect(iter_reg_input, 'image',
                        self.workflow, 'input_spec.moving_image')
        exec_wf.connect(iter_reg_input, 'reference',
                        self.workflow, 'input_spec.reference')

        # The output destination directory.
        if not os.path.exists(dest):
            os.makedirs(dest)

        # Copy the pre-arrival files to the destination directory.
        copy_pre_arrival_func = Function(
            input_names=['in_files', 'dest'],
            output_names=['out_files'], function=copy_files)
        copy_pre_arrival = pe.Node(copy_pre_arrival_func, dest=dest,
                                   name='copy_pre_arrival')
        # Work around the following Nipype bug:
        # * the Function Node input is not set by the Node constructor
        if not copy_pre_arrival.inputs.dest:
            copy_pre_arrival.inputs.dest = dest
        exec_wf.connect(input_spec, 'pre_arrival', copy_pre_arrival, 'in_files')

        # Copy the realigned image to the destination directory.
        copy_realigned = pe.Node(Copy(dest=dest), name='copy_realigned')
        exec_wf.connect(self.workflow, 'output_spec.out_file',
                        copy_realigned, 'in_file')

        # Set the recursive realigned -> reference connections.
        connect_ref_args = dict(bolus_arrival_index=bolus_arrival_index,
                                initial_reference=ref_0_image)
        exec_wf.connect_iterables(copy_realigned, iter_reg_input,
                                  connect_reference, **connect_ref_args)

        # Collect the realigned images.
        collect_realigned = pe.JoinNode(IdentityInterface(fields=['images']),
                                        joinsource='iter_reg_input',
                                        joinfield='images',
                                        name='collect_realigned')
        exec_wf.connect(copy_realigned, 'out_file', collect_realigned, 'images')

        # Merge the pre-arrival and realigned images.
        collect_reg = pe.Node(Merge(2), name='collect_reg')
        exec_wf.connect(copy_pre_arrival, 'out_files', collect_reg, 'in1')
        exec_wf.connect(collect_realigned, 'images', collect_reg, 'in2')

        # Upload the registration result into the XNAT registration resource.
        upload_reg_xfc = XNATUpload(project=self.project, resource=self.resource,
                                    modality='MR')
        upload_reg = pe.Node(upload_reg_xfc, name='upload_reg')
        exec_wf.connect(input_spec, 'subject', upload_reg, 'subject')
        exec_wf.connect(input_spec, 'session', upload_reg, 'session')
        exec_wf.connect(input_spec, 'scan', upload_reg, 'scan')
        exec_wf.connect(collect_reg, 'out', upload_reg, 'in_files')

        # The execution output.
        output_spec = pe.Node(IdentityInterface(fields=['images']),
                              name='output_spec')
        exec_wf.connect(collect_reg, 'out', output_spec, 'images')

        self._logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
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
        :keyword base_dir: the workflow execution directory
            (default is a new temp directory)
        :keyword technique: the registration technique
            (``ants``, `fnirt`` or ``mock``, default ``ants``)
        :return: the Workflow object
        """
        self._logger.debug('Creating the registration realignment workflow...')

        # The workflow.
        base_dir = opts.get('base_dir', tempfile.mkdtemp())
        realign_wf = pe.Workflow(name='registration', base_dir=base_dir)

        # The workflow input.
        in_fields = ['subject', 'session', 'scan', 'moving_image', 'reference',
                     'mask', 'resource']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')

        # The realign workflow.
        technique = opts.get('technique')
        if not technique:
            technique = 'ants'
        input_spec.inputs.resource = self.resource

        # Copy the DICOM meta-data. The copy target is set by the technique
        # node defined below.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        realign_wf.connect(input_spec, 'moving_image', copy_meta, 'src_file')

        if technique.lower() == 'ants':
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
            # ANTS transformations.
            register = pe.Node(Registration(**metric_inputs), name='register')
            realign_wf.connect(input_spec, 'reference',
                               register, 'fixed_image')
            realign_wf.connect(input_spec, 'moving_image',
                               register, 'moving_image')
            realign_wf.connect(input_spec, 'mask',
                               register, 'fixed_image_mask')
            realign_wf.connect(input_spec, 'mask',
                               register, 'moving_image_mask')
            # Get the file name without directory.
            input_filename_xfc = Function(input_names=['in_file'],
                                          output_names=['out_file'],
                                          function=filename)
            input_filename = pe.Node(input_filename_xfc, name='input_filename')
            realign_wf.connect(input_spec, 'moving_image',
                               input_filename, 'in_file')
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
        elif technique.lower() == 'fnirt':
            # Copy the input to a work directory, since FNIRT adds
            # temporary files to the input image location.
            fnirt_copy_moving = pe.Node(Copy(), name='fnirt_copy_moving')
            realign_wf.connect(input_spec, 'moving_image',
                               fnirt_copy_moving, 'in_file')
            # Get the affine transformation.
            flirt = pe.Node(fsl.FLIRT(), name='flirt')
            realign_wf.connect(input_spec, 'reference', flirt, 'reference')
            realign_wf.connect(fnirt_copy_moving, 'out_file', flirt, 'in_file')
            # Register the image.
            fnirt = pe.Node(fsl.FNIRT(), name='fnirt')
            realign_wf.connect(input_spec, 'reference', fnirt, 'ref_file')
            realign_wf.connect(flirt, 'out_matrix_file', fnirt, 'affine_file')
            realign_wf.connect(fnirt_copy_moving, 'out_file', fnirt, 'in_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'inmask_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'refmask_file')
            # Copy the meta-data.
            realign_wf.connect(fnirt, 'warped_file', copy_meta, 'dest_file')
        elif technique.lower() == 'mock':
            # Copy the input scan file to an output file.
            mock_copy = pe.Node(Copy(), name='mock_copy')
            realign_wf.connect(input_spec, 'moving_image',
                               mock_copy, 'in_file')
            realign_wf.connect(mock_copy, 'out_file', copy_meta, 'dest_file')
        else:
            raise PipelineError("Registration technique not recognized: %s" %
                             technique)

        # The output is the realigned image.
        output_spec = pe.Node(IdentityInterface(fields=['out_file']),
                              name='output_spec')
        realign_wf.connect(copy_meta, 'dest_file', output_spec, 'out_file')

        self._configure_nodes(realign_wf)

        self._logger.debug("Created the %s workflow." % realign_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self.depict_workflow(realign_wf)
        return realign_wf


### Utility functions called by the workflow nodes. ###

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


def connect_reference(workflow, realigned_nodes, input_nodes,
                      bolus_arrival_index, initial_reference):
    """
    Connects the reference input for the given reference workflow input
    nodes as follows:

    * Prior to bolus arrival, the reference is the successor realignment.

    * The reference for the nodes before and after the given initial
      reference node is that initial reference.

    * The bolus arrival successor node reference is the *initial_reference*.

    The initial reference is the input node for the volume immediately
    following bolus uptake. This node is not included in the iterable
    expansion nodes.

    :param workflow: the workflow delegate which connects nodes
    :param ref_nodes: the iterable expansion reference input nodes
    :param realigned_nodes: the iterable expansion realignment output nodes
    :param bolus_arrival_index: the bolus uptake volume index
    :param initial_reference: the starting reference input node
    """
    # The number of input and realigned nodes.
    node_cnt = len(input_nodes)
    # The reference is the successor realignment up to the bolus arrival.
    end = min(bolus_arrival_index - 1, node_cnt - 1)
    for i in range(0, end):
        workflow.connect(realigned_nodes[i + 1], 'out_file',
                         input_nodes[i], 'reference')

    # The reference for the bolus arrival predecessor and successor
    # is the initial reference.
    start = max(bolus_arrival_index - 1, 0)
    end = min(bolus_arrival_index + 1, node_cnt)
    for i in range(start, end):
        input_nodes[i].inputs.reference = initial_reference

    # The reference is the predecessor realignment for the remaining
    # nodes.
    for i in range(bolus_arrival_index, node_cnt - 1):
        workflow.connect(realigned_nodes[i], 'out_file',
                         input_nodes[i + 1], 'reference')
