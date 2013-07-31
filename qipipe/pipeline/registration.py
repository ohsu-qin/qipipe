import os, re
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import AverageImages, ApplyTransforms
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import DcmStack, CopyMeta
from ..helpers.project import project
from ..interfaces import XNATDownload, XNATUpload
from ..helpers import xnat_helper, file_helper
from .workflow_base import WorkflowBase
from .mask import MaskWorkflow

import logging
logger = logging.getLogger(__name__)

REG_PREFIX = 'reg'
"""The XNAT registration reconstruction name prefix."""

MASK_RECON = 'mask'
"""The XNAT mask reconstruction name."""


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.registration.RegistrationWorkflow`
    and runs it on the given inputs.
    
    :param inputs: the :meth:`qipipe.pipeline.registrationRegistrationWorkflow.run`
        inputs
    :param opts: the :class:`qipipe.pipeline.registrationRegistrationWorkflow`
        initializer options
    :return: the :meth:`qipipe.pipeline.registrationRegistrationWorkflow.run`
        result
    """
    return RegistrationWorkflow(**opts).run(*inputs)


class RegistrationWorkflow(WorkflowBase):
    """
    The RegistrationWorkflow class builds and executes the registration workflow.
    
    The workflow registers the images as follows:
    
    - Make a fixed reference image
    
    - Register each image against the reference image
    
    - Upload the registered images
    
    The registration workflow input is the ``input_spec`` node consisting of the
    following input fields:
    
    - `subject`: the subject name
    
    - `session`: the session name
    
    - `mask`: the mask to apply to the images
    
    - `images`: the session images to register
    
    In addition, the iterable `image_iter` node input field `image` must
    be set to the input session images.
    
    The mask can be obtained by running the :class:`qipipe.pipeline.mask.MaskWorkflow`
    workflow.
    
    The registration workflow output is the ``output_spec`` node consisting of the
    following output fields:
    
    - `out_file`: the realigned image file
    
    - `reconstruction`: the registration XNAT reconstruction name
    
    The `reconstruction` output value is a unique value used for all
    workflow runs against this
    :class:`qipipe.pipeline.registration.RegistrationWorkflow` instance.
    
    There are two registration workflow paths:
    
    - a path which uses the images list as an input to create the mask and
        registration reference image
    
    - a path which uses each image as an input to realign the image
    
    The image realign path uses outputs from the mask/reference path.
    
    The fixed reference image is the average of the middle three input images.
    
    Two registration techniques are supported:
    
    - ANTS_ SyN_ symmetric normalization diffeomorphic registration (default)
    
    - FSL_ FNIRT_ non-linear registration
    
    The optional workflow configuration file can contain the following sections:
    
    - ``ANTSAverage``: the ANTS `Average interface`_ options
    
    - ``ANTSRegistration``: the ANTS `Registration interface`_ options
    
    - ``ANTSApplyTransforms``: the ANTS `ApplyTransform interface`_ options
    
    - ``FSLFNIRT``: the FSL `FNIRT interface`_ options
    
    .. _ANTS: http://stnava.github.io/ANTs/
    .. _ApplyTransform interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.resampling.html
    .. _Average interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.utils.html
    .. _FNIRT: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FNIRT#Research_Overview
    .. _FNIRT interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.fsl.preprocess.html
    .. _FSL: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FSL
    .. _Registration interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.registration.html
    .. _SyN: http://www.ncbi.nlm.nih.gov/pubmed/17659998
    """
    
    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow settings in
        that file override the default settings.
        
        :keyword base_dir: the workflow execution directory (default a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword technique: the case-insensitive workflow technique
            (``ANTS`` or ``FNIRT``, default ``ANTS``)
        """
        super(RegistrationWorkflow, self).__init__(logger, opts.pop('cfg_file', None))
        
        self.reusable_workflow = self._create_reusable_workflow(**opts)
        """The reusable registration workflow."""
        
        self._reg_mask_dl_wf = self._create_workflow_with_existing_mask(**opts)
        """The execution workflow to use with a existing mask."""
        
        self._reg_mask_cr_wf = self._create_workflow_with_nonexisting_mask(**opts)
        """The execution workflow to use with a non-existing mask."""
        
        self._reconstruction = self._generate_reconstruction_name()
        """The XNAT reconstruction name used for all runs against this workflow
        instance."""
    
    def run(self, *inputs):
        """
        Runs the registration workflow on the given inputs.
        
        The NiFTI scan images for each session are downloaded from XNAT into the
        ``scans`` subdirectory of the ``base_dir`` specified in the
        :class:`RegistrationWorkflow` initializer options (default is the current
        directory). The workflow is run on these images, resulting in a new XNAT
        reconstruction object for each session which contains the realigned images.
        
        :param inputs: the (subject, session) name tuples to register
        :return: the registration reconstruction name, unqualified by the
            session parent label
        """
        
        # Run the workflow on each session.
        for sbj, sess in inputs:
            self._register(sbj, sess)
        
        return self._reconstruction
    
    def _generate_reconstruction_name(self):
        """
        Makes a unique registration reconstruction name for the registration
        workflow input. Uniqueness permits more than one registration to be
        stored without a name conflict.
        
        :return: a unique XNAT registration reconstruction name
        """
        return "%s_%s" % (REG_PREFIX, file_helper.generate_file_name())
    
    def _register(self, subject, session):
        """
        Runs a registration execution workflow on the given session.
        
        :param subject: the subject name
        :param session: the session name
        :param recon: the reconstruction name
        """
        # The scan series stack download location.
        base_dir = self._reg_mask_cr_wf.base_dir or os.getcwd()
        dest = os.path.join(base_dir, 'data', subject, session)
        
        # Download the scan images. This step cannot be done within the workflow,
        # since Nipype requires that the workflow iterator is bound to the images
        # when the workflow is built rather than set dynamically by a download
        # step.
        with xnat_helper.connection() as xnat:
            images = self._download_scans(xnat, subject, session, dest)
        # Sort the images by series number.
        images.sort()
        
        # The workflow to use depends on whether there is a mask.
        reg_wf = self._execution_workflow_for(subject, session)
        
        # Execute the registration workflow.
        self._set_registration_input(reg_wf, subject, session, images)
        logger.debug("Executing the %s workflow on %s %s..." %
            (reg_wf.name, subject, session))
        self._run_workflow(reg_wf)
        logger.debug("%s %s is registered as reconstruction %s." %
            (subject, session, recon))
    
    def _execution_workflow_for(self, subject, session):
        """
        Returns the registration execution workflow to use for the given session.
        
        :param subject: the subject name
        :param session: the session name
        :return: the workflow to use
        """
        # The workflow to run is determined by whether there is an existing mask.
        with xnat_helper.connection() as xnat:
            mask = xnat.get_reconstruction(project(), subject, session, MASK_RECON)
            if mask.exists():
                logger.debug("The %s mask exisits." % mask.label())
                return self._reg_mask_dl_wf
            else:
                return self._reg_mask_cr_wf
    
    def _set_registration_input(self, workflow, subject, session, images):
        """
        Sets the registration input.
        
        :param workflow: the registration execution workflow
        :param subject: the subject name
        :param session: the session name
        :param images: the scan images to register
        """
        # Set the workflow inputs.
        input_spec = workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.images = images
        
        # The images are iterable in the realign workflow.
        image_iter = workflow.get_node('image_iter')
        abs_images = [os.path.abspath(fname) for fname in images]
        image_iter.iterables = dict(image=abs_images).items()
    
    def _create_workflow_with_existing_mask(self, **opts):
        logger.debug("Creating the registration workflow to use with a existing mask...")
        
        # The reusable registration workflow.
        base_wf = self.reusable_workflow.clone(name='reg_dl_mask_base')
        
        # The execution workflow.
        exec_wf = pe.Workflow(name='reg_dl_mask_exec', base_dir=opts.get('base_dir'))
        
        # The execution workflow input.
        in_fields = ['subject', 'session', 'images']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # The image iterator.
        image_iter = pe.Node(IdentityInterface(fields=['image']), name='image_iter')
        exec_wf.connect(image_iter, 'image', base_wf, 'image_iter.image')
        
        # Download the mask.
        dl_mask = pe.Node(XNATDownload(project=project(), reconstruction=MASK_RECON),
            name='dl_mask')
        exec_wf.connect(input_spec, 'subject', dl_mask, 'subject')
        exec_wf.connect(input_spec, 'session', dl_mask, 'session')
        
        # Connect the reusable workflow inputs.
        input_spec = base_wf.get_node('input_spec')
        exec_wf.connect(input_spec, 'subject', base_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', base_wf, 'input_spec.session')
        exec_wf.connect(input_spec, 'images', base_wf, 'input_spec.images')
        exec_wf.connect(dl_mask, 'out_file', base_wf, 'input_spec.mask')
        
        logger.debug("Created the %s workflow." % exec_wf.name)
        
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(exec_wf)
        
        return exec_wf
    
    def _create_workflow_with_nonexisting_mask(self, **opts):
        logger.debug("Creating the registration workflow to use with a non-existing mask...")
        
        # The mask creation workflow.
        mask_wf = MaskWorkflow(base_dir=opts.get('base_dir')).workflow.clone(name='reg_mask')
        
        # The reusable registration workflow.
        base_wf = self.reusable_workflow.clone(name='reg_cr_mask_base')
        
        # The execution workflow.
        exec_wf = pe.Workflow(name='reg_cr_mask_exec', base_dir=opts.get('base_dir'))
        
        # The execution workflow input.
        in_fields = ['subject', 'session', 'images']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # The image iterator.
        image_iter = pe.Node(IdentityInterface(fields=['image']), name='image_iter')
        exec_wf.connect(image_iter, 'image', base_wf, 'image_iter.image')
        
        # Connect the mask workflow inputs.
        exec_wf.connect(input_spec, 'subject', mask_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', mask_wf, 'input_spec.session')
        exec_wf.connect(input_spec, 'images', mask_wf, 'input_spec.images')
        
        # Connect the reusable workflow inputs.
        input_spec = base_wf.get_node('input_spec')
        exec_wf.connect(input_spec, 'subject', base_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', base_wf, 'input_spec.session')
        exec_wf.connect(input_spec, 'images', base_wf, 'input_spec.images')
        exec_wf.connect(mask_wf, 'output_spec.out_file', base_wf, 'input_spec.mask')
        
        logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(exec_wf)
        
        return exec_wf
    
    def _create_reusable_workflow(self, name, **opts):
        """
        Creates the base registration workflow. The registration workflow performs the
        following steps:
        
        - Generates a unique XNAT reconstruction name
        
        - Set the mask and realign workflow inputs
        
        - Run these workflows
        
        - Upload the mask and realign outputs to XNAT
        
        :param name: the workflow name
        :param opts: the following workflow options:
        :keyword base_dir: the workflow execution directory (default current directory)
        :keyword technique: the registration technique
            ('``ANTS`` or ``FNIRT``, default ``ANTS``)
        :return: the Workflow object
        """
        logger.debug("Creating a base registration workflow...")
        
        # The reusable workflow.
        base_wf = pe.Workflow(name=name, base_dir=opts.get('base_dir'))
        
        # The workflow input.
        in_fields = ['subject', 'session', 'images', 'mask']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        # The same reconstruction name is used for all runs
        # against this instance.
        input_spec.inputs.reconstruction = self._reconstruction
        
        # The average options.
        avg_opts = self.configuration.get('ANTSAverage', {})
        # Make the reference image.
        average = pe.Node(AverageImages(**avg_opts), name='average')
        # The average is taken over the middle three images.
        base_wf.connect(input_spec, ('images', _middle, 3), average, 'images')
        
        # Mask the reference image.
        mask_ref = pe.Node(fsl.maths.ApplyMask(output_type='NIFTI_GZ'), name='mask_ref')
        base_wf.connect(average, 'output_average_image', mask_ref, 'in_file')
        base_wf.connect(input_spec, 'mask', mask_ref, 'mask_file')
        
        # The realign image iterator.
        image_iter = pe.Node(IdentityInterface(fields=['image']), name='image_iter')
        
        # The realign workflow.
        realign_wf = self._create_realign_workflow(**opts)
        
        # Register and resample the images.
        base_wf.connect(input_spec, 'subject', realign_wf, 'input_spec.subject')
        base_wf.connect(input_spec, 'session', realign_wf, 'input_spec.session')
        base_wf.connect(image_iter, 'image', realign_wf, 'input_spec.moving_image')
        base_wf.connect(mask_ref, 'out_file', realign_wf, 'input_spec.mask')
        base_wf.connect(average, 'output_average_image', realign_wf, 'input_spec.fixed_image')
        
        # Upload the realigned image to XNAT.
        upload_reg = pe.Node(XNATUpload(project=project(), format='NIFTI'),
            name='upload_reg')
        base_wf.connect(input_spec, 'subject', upload_reg, 'subject')
        base_wf.connect(input_spec, 'session', upload_reg, 'session')
        base_wf.connect(input_spec, 'reconstruction', upload_reg, 'reconstruction')
        base_wf.connect(realign_wf, 'output_spec.out_file', upload_reg, 'in_files')
        
        # The workflow output is the reconstruction name and the realigned image file.
        realign_output = realign_wf.get_node('output_spec')
        out_fields = ['out_file', 'reconstruction']
        output_spec = pe.Node(IdentityInterface(fields=out_fields), name='output_spec')
        base_wf.connect(realign_wf, 'output_spec.out_file', output_spec, 'out_file')
        
        
        return base_wf

    def _create_realign_workflow(self, base_dir=None, technique='ANTS'):
        """
        Creates the workflow which registers and resamples images.
        
        :param base_dir: the workflow execution directory (default is the current directory)
        :param technique: the registration technique (``ANTS`` or ``FNIRT``)
        :return: the Workflow object
        """
        logger.debug('Creating the realign workflow...')
        
        workflow = pe.Workflow(name='realign', base_dir=base_dir)
        
        # The workflow input image iterator.
        in_fields = ['subject', 'session', 'mask', 'fixed_image', 'moving_image', 'reconstruction']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # Make the realigned image file name.
        realign_name_func = Function(input_names=['reconstruction', 'in_file'],
            output_names=['out_file'],
            function=_gen_realign_filename)
        realign_name = pe.Node(realign_name_func, name='realign_name')
        workflow.connect(input_spec, 'reconstruction', realign_name, 'reconstruction')
        workflow.connect(input_spec, 'moving_image', realign_name, 'in_file')
        
        # Copy the DICOM meta-data. The copy target is set by the technique
        # node defined below.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        workflow.connect(input_spec, 'moving_image', copy_meta, 'src_file')
        
        if not technique or technique.lower() == 'ants':
            # The ANTS registration options.
            reg_opts = self.configuration.get('ANTSRegistration', {})
            # Register the images to create the warp and affine transformations.
            register = pe.Node(Registration(**reg_opts), name='register')
            workflow.connect(input_spec, 'fixed_image', register, 'fixed_image')
            workflow.connect(input_spec, 'moving_image', register, 'moving_image')
            workflow.connect(input_spec, 'mask', register, 'fixed_image_mask')
            workflow.connect(input_spec, 'mask', register, 'moving_image_mask')
            # The ANTS realign options.
            apply_opts = self.configuration.get('ANTSApplyTransforms', {})
            # Apply the transforms to the input image.
            apply_xfm = pe.Node(ApplyTransforms(**apply_opts), name='apply_xfm')
            workflow.connect(input_spec, 'fixed_image', apply_xfm, 'reference_image')
            workflow.connect(input_spec, 'moving_image', apply_xfm, 'input_image')
            workflow.connect(realign_name, 'out_file', apply_xfm, 'output_image')
            workflow.connect(register, 'forward_transforms', apply_xfm, 'transforms')
            # Copy the meta-data.
            workflow.connect(apply_xfm, 'output_image', copy_meta, 'dest_file')
        elif technique.lower() == 'fnirt':
            # The FNIRT registration options.
            fnirt_opts = self.configuration.get('FSLFNIRT', {})
            # Register the images.
            fnirt = pe.Node(fsl.FNIRT(**fnirt_opts), name='fnirt')
            workflow.connect(input_spec, 'fixed_image', fnirt, 'ref_file')
            workflow.connect(input_spec, 'moving_image', fnirt, 'in_file')
            workflow.connect(input_spec, 'mask', fnirt, 'inmask_file')
            workflow.connect(input_spec, 'mask', fnirt, 'refmask_file')
            workflow.connect(realign_name, 'out_file', fnirt, 'warped_file')
            # Copy the meta-data.
            workflow.connect(fnirt, 'warped_file', copy_meta, 'dest_file')
        else:
            raise PipelineError("Registration technique not recognized: %s" % technique)
        
        # The output is the realigned images.
        output_spec = pe.Node(IdentityInterface(fields=['out_file']), name='output_spec')
        workflow.connect(copy_meta, 'dest_file', output_spec, 'out_file')
        
        return workflow


### Utility functions called by workflow nodes. ###

SPLITEXT_PAT = re.compile("""
    (.*?)           # The file path without the extension
    (               # The extension group
        (\.\w+)+    # The (possibly composite) extension
    )?              # The extension is optional
    $               # Anchor to the end of the file path
    """, re.VERBOSE)
"""
Regexp pattern that splits the name and extension.
Unlike ``os.path.splitext``, this pattern captures a composite extension, e.g.:
>>> import os
>>> os.path.splitext('/tmp/foo.nii.gz')
('/tmp/foo.nii', '.gz')
>>> FILENAME_SPLITTER_PAT.match('/tmp/foo.3/bar.nii.gz').groups()
('/tmp/foo.3/bar', '.nii.gz')
"""

def _gen_realign_filename(reconstruction, in_file):
    """
    :param reconstruction: the reconstruction name
    :param in_file: the input scan image filename
    :return: the registered image filename
    """
    from qipipe.pipeline.registration import SPLITEXT_PAT
    
    groups = SPLITEXT_PAT.match(in_file).groups()
    base = groups[0]
    fname = "%s_%s" % (base, reconstruction)
    if len(groups) == 1:
        return fname
    else:
        ext = groups[1]
        return fname + ext

def _gen_mask_filename(subject, session):
    return "%s_%s_mask.nii.gz" % (subject.lower(), session.lower())

def _middle(items, proportion_or_length):
    """
    Returns a sublist of the given items determined as follows:
    
    - If ``proportion_or_length`` is a float, then the middle fraction
        given by that parameter
    
    - If ``proportion_or_length`` is an integer, then the middle
        items with length given by that parameter
    
    :param items: the list of items to subset
    :param proportion_or_length: the fraction or number of middle items
        to select
    :return: the middle items
    """
    if proportion_or_length < 0:
        raise ValueError("The _middle proportion_or_length parameter is not"
            " a non-negative number: %s" % proportion_or_length)
    elif isinstance(proportion_or_length, float):
        if proportion_or_length > 1:
            raise ValueError("The _middle proportion parameter cannot"
                " exceed 1.0: %s" % proportion_or_length)
        offset = int(len(items) * (proportion_or_length / 2))
    elif isinstance(proportion_or_length, int):
        offset = max(0, int(proportion_or_length / 2))
    else:
        raise ValueError("The _middle proportion_or_length parameter is not"
            " a number: %s" % proportion_or_length)
    
    return sorted(items)[offset:len(items)-offset]
