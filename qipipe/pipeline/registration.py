import os
import re
import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces.ants import (
    AverageImages, Registration, ApplyTransforms)
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import CopyMeta
from .. import project
from ..interfaces import XNATUpload
from ..helpers import file_helper
from .workflow_base import WorkflowBase
from ..helpers.logging_helper import logger


REG_PREFIX = 'reg'
"""The XNAT registration reconstruction name prefix."""


def run(input_dict, **opts):
    """
    Creates a :class:`qipipe.pipeline.registration.RegistrationWorkflow`
    and runs it on the given inputs.
    
    :param input_dict: the :meth:`qipipe.pipeline.registration.RegistrationWorkflow.run`
        inputs
    :param opts: the :class:`qipipe.pipeline.registration.RegistrationWorkflow`
        initializer and :meth:`qipipe.pipeline.registration.RegistrationWorkflow.run`
        options
    :return: the :meth:`qipipe.pipeline.registration.RegistrationWorkflow.run`
        result
    """
    return RegistrationWorkflow(**opts).run(input_dict)


class RegistrationWorkflow(WorkflowBase):

    """
    The RegistrationWorkflow class builds and executes the registration workflow.
    The workflow registers an input NiFTI scan image against the input reference
    image and uploads the realigned image to XNAT.
    
    The registration workflow input is the *input_spec* node consisting of the
    following input fields:
    
    - *subject*: the subject name
    
    - *session*: the session name
    
    - *mask*: the mask to apply to the images
    
    - *reference*: the fixed reference image
    
    - *image*: the image file to register
    
    The mask can be obtained by running the
    :class:`qipipe.pipeline.mask.MaskWorkflow` workflow.
    
    The reference can be obtained by running the
    :class:`qipipe.pipeline.reference.ReferenceWorkflow` workflow.
    
    The registration workflow output is the *output_spec* node consisting of
    the following output field:
    
    - *image*: the realigned image file
    
    Two registration techniques are supported:
    
    - ANTS_ SyN_ symmetric normalization diffeomorphic registration (default)
    
    - FSL_ FNIRT_ non-linear registration
    
    The optional workflow configuration file can contain overrides for the
    Nipype interface inputs in the following sections:
    
    - ``ants.Registration``: the ANTS `Registration interface`_ options
    
    - ``ants.ApplyTransforms``: the ANTS `ApplyTransform interface`_ options
    
    - ``fsl.FNIRT``: the FSL `FNIRT interface`_ options
    
    :Note: Since the XNAT *reconstruction* name is unique, a
        :class:`qipipe.pipeline.registration.RegistrationWorkflow` instance
        can be used for only one registration workflow. Different registration
        inputs require different
        :class:`qipipe.pipeline.registration.RegistrationWorkflow` instances.
    
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
        
        :param opts: the following initialization options:
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword reconstruction: the XNAT reconstruction name to use
        :keyword technique: the case-insensitive workflow technique
            (``ANTS`` or ``FNIRT``, default ``ANTS``)
        """
        super(RegistrationWorkflow, self).__init__(logger(__name__),
                                                   opts.pop('cfg_file', None))

        recon = opts.pop('reconstruction', None)
        if not recon:
            recon = self._generate_reconstruction_name()
        self.reconstruction = recon
        """The XNAT reconstruction name used for all runs against this workflow
        instance."""

        self.workflow = self._create_workflow(**opts)
        """The registration workflow."""

    def run(self, input_dict):
        """
        Runs the registration workflow on the given inputs.
        
        The NiFTI scan images for each session are downloaded from XNAT into the
        ``scans`` subdirectory of the *base_dir* specified in the
        :class:`RegistrationWorkflow` initializer options (default is the current
        directory). The workflow is run on these images, resulting in a new XNAT
        reconstruction object for each session which contains the realigned images.
        
        :param input_dict: the input
            *{subject: {session: ([images], mask, reference)}}* to register
        :param mask: the mask file
        :return: the registration XNAT reconstruction name
        """
        # The number of sessions.
        sess_cnt = sum(map(len, input_dict.values()))
        # The [[(images, mask])]] list.
        sess_tuples = [sess_dict.values()
                       for sess_dict in input_dict.itervalues()]
        # The flattened [(images, mask])] list.
        sbj_tuples = reduce(lambda x, y: x + y, sess_tuples)
        # The [images] list of lists.
        images_list = [images for images, _ in sbj_tuples]
        # The number of images.
        img_cnt = sum(map(len, images_list))

        # Register the images.
        self._logger.debug("Registering %d images in %d sessions..." %
                           (img_cnt, sess_cnt))
        for sbj, sess_dict in input_dict.iteritems():
            self._logger.debug("Registering subject %s..." % sbj)
            for sess, sess_inputs in sess_dict.iteritems():
                images, mask, reference = sess_inputs
                self._logger.debug("Registering %d %s %s images" %
                                 (len(images), sbj, sess))
                self._register(sbj, sess, images, mask)
                self._logger.debug("Registered %s %s." % (sbj, sess))
            self._logger.debug("Registered subject %s." % sbj)
        self._logger.debug("Registered %d images from %d sessions." %
                         (img_cnt, sess_cnt))

        return self.reconstruction

    def _generate_reconstruction_name(self):
        """
        Makes a unique registration reconstruction name for the registration
        workflow input. Uniqueness permits more than one registration to be
        stored without a name conflict.
        
        :return: a unique XNAT registration reconstruction name
        """
        return "%s_%s" % (REG_PREFIX, file_helper.generate_file_name())

    def _register(self, subject, session, images, mask, reference):
        """
        Runs the registration workflow on the given session images.
        
        :param subject: the subject name
        :param session: the session name
        :param images: the input session images
        :param mask: the image mask
        :param reference: the fixed reference image
        """
        # Sort the images by series number, since the fixed reference image
        # is an average of several middle series.
        images = sorted(images)

        # Set the workflow input.
        self._set_registration_input(subject, session, images, mask, reference)
        # Execute the registration workflow.
        self._logger.debug("Executing the %s workflow on %s %s..." %
                         (self.workflow.name, subject, session))
        self._run_workflow(self.workflow)
        self._logger.debug("Executed the %s workflow on %s %s." %
                         (self.workflow.name, subject, session))

    def _set_registration_input(self, subject, session, images, mask, reference):
        """
        Sets the registration input.
        
        :param subject: the subject name
        :param session: the session name
        :param images: the scan images to register
        :param mask: the image mask
        :param reference: the fixed reference image
        """
        # Validate the mask input.
        if not mask:
            raise ValueError("The mask option is required for registration.")
        # Validate the reference input.
        if not reference:
            raise ValueError("The reference option is required for registration.")

        # Set the workflow inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.mask = mask
        input_spec.inputs.reference = reference

        # The images are iterable.
        abs_images = [os.path.abspath(fname) for fname in images]
        input_spec.iterables = ('image', abs_images)

    def _create_workflow(self, **opts):
        """
        Creates the base registration workflow. The registration workflow
        performs the following steps:
        
        - Generates a unique XNAT reconstruction name
        
        - Set the mask and realign workflow inputs
        
        - Run these workflows
        
        - Upload the realign outputs to XNAT
        
        :param opts: the following workflow options:
        :keyword base_dir: the workflow execution directory
            (default is a new temp directory)
        :keyword technique: the registration technique
            ('``ANTS`` or ``FNIRT``, default ``ANTS``)
        :return: the Workflow object
        """
        self._logger.debug("Creating the registration workflow...")

        # The workflow.
        base_dir = opts.get('base_dir', tempfile.mkdtemp())
        reg_wf = pe.Workflow(name='registration', base_dir=base_dir)

        # The workflow input.
        in_fields = ['subject', 'session', 'mask', 'reference', 'image']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')

        # The realign workflow.
        technique = opts.get('technique')
        realign_wf = self._create_realign_workflow(base_dir, technique)

        # Register and resample the image.
        reg_wf.connect(input_spec, 'subject',
                       realign_wf, 'input_spec.subject')
        reg_wf.connect(input_spec, 'session',
                       realign_wf, 'input_spec.session')
        reg_wf.connect(input_spec, 'mask',
                       realign_wf, 'input_spec.mask')
        reg_wf.connect(input_spec, 'reference',
                       realign_wf, 'input_spec.reference')
        reg_wf.connect(input_spec, 'image',
                       realign_wf, 'input_spec.moving_image')

        # Upload the realigned image to XNAT.
        upload_reg_xfc = XNATUpload(project=project(),
                                    reconstruction=self.reconstruction,
                                    format='NIFTI')
        upload_reg = pe.Node(upload_reg_xfc, name='upload_reg')
        reg_wf.connect(input_spec, 'subject', upload_reg, 'subject')
        reg_wf.connect(input_spec, 'session', upload_reg, 'session')
        reg_wf.connect(realign_wf, 'output_spec.out_file',
                       upload_reg, 'in_files')

        # The workflow output is the realigned image file.
        output_spec = pe.Node(IdentityInterface(fields=['image']),
                              name='output_spec')
        reg_wf.connect(realign_wf, 'output_spec.out_file',
                       output_spec, 'image')

        self._configure_nodes(reg_wf)

        self._logger.debug("Created the %s workflow." % reg_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self._depict_workflow(reg_wf)

        return reg_wf

    def _create_realign_workflow(self, base_dir, technique=None):
        """
        Creates the workflow which registers and resamples images.
        
        :param base_dir: the workflow execution directory
        :param technique: the registration technique (``ANTS`` or ``FNIRT``,
            default ``ANTS``)
        :return: the Workflow object
        """
        self._logger.debug('Creating the realign workflow...')

        realign_wf = pe.Workflow(name='realign', base_dir=base_dir)
        
        if not technique:
            technique = 'ANTS'

        # The workflow input image iterator.
        in_fields = ['subject', 'session', 'mask', 'reference',
                     'moving_image', 'reconstruction']
        input_spec = pe.Node(
            IdentityInterface(fields=in_fields), name='input_spec')
        input_spec.inputs.reconstruction = self.reconstruction

        # Make the realigned image file name.
        realign_name_func = Function(input_names=['reconstruction', 'in_file'],
                                     output_names=['out_file'],
                                     function=_gen_realign_filename)
        realign_name = pe.Node(realign_name_func, name='realign_name')
        realign_wf.connect(
            input_spec, 'reconstruction', realign_name, 'reconstruction')
        realign_wf.connect(input_spec, 'moving_image', realign_name, 'in_file')

        # Copy the DICOM meta-data. The copy target is set by the technique
        # node defined below.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        realign_wf.connect(input_spec, 'moving_image', copy_meta, 'src_file')

        if technique.lower() == 'ants':
            # Setting the registration metric and metric_weight inputs after the
            # node is created results in a Nipype input trait dependency warning.
            # Avoid this warning by setting these inputs in the constructor
            # from the values in the configuration.
            reg_cfg = self._interface_configuration(Registration)
            metric_inputs = {field: reg_cfg[field]
                             for field in ['metric', 'metric_weight']
                             if field in reg_cfg}
            # Register the images to create the warp and affine
            # transformations.
            register = pe.Node(Registration(**metric_inputs), name='register')
            realign_wf.connect(
                input_spec, 'reference', register, 'fixed_image')
            realign_wf.connect(
                input_spec, 'moving_image', register, 'moving_image')
            realign_wf.connect(
                input_spec, 'mask', register, 'fixed_image_mask')
            realign_wf.connect(
                input_spec, 'mask', register, 'moving_image_mask')
            # Apply the transforms to the input image.
            apply_xfm = pe.Node(ApplyTransforms(), name='apply_xfm')
            realign_wf.connect(
                input_spec, 'reference', apply_xfm, 'reference_image')
            realign_wf.connect(
                input_spec, 'moving_image', apply_xfm, 'input_image')
            realign_wf.connect(
                realign_name, 'out_file', apply_xfm, 'output_image')
            realign_wf.connect(
                register, 'forward_transforms', apply_xfm, 'transforms')
            # Copy the meta-data.
            realign_wf.connect(
                apply_xfm, 'output_image', copy_meta, 'dest_file')
        elif technique.lower() == 'fnirt':
            # Register the image.
            fnirt = pe.Node(fsl.FNIRT(), name='fnirt')
            realign_wf.connect(input_spec, 'reference', fnirt, 'ref_file')
            realign_wf.connect(input_spec, 'moving_image', fnirt, 'in_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'inmask_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'refmask_file')
            realign_wf.connect(realign_name, 'out_file', fnirt, 'warped_file')
            # Copy the meta-data.
            realign_wf.connect(fnirt, 'warped_file', copy_meta, 'dest_file')
        else:
            raise ValueError("Registration technique not recognized: %s" %
                             technique)

        # The output is the realigned image.
        output_spec = pe.Node(IdentityInterface(fields=['out_file']),
                              name='output_spec')
        realign_wf.connect(copy_meta, 'dest_file', output_spec, 'out_file')

        self._configure_nodes(realign_wf)

        return realign_wf


# Utility functions called by workflow nodes. ###

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
>>> SPLITEXT_PAT.match('/tmp/foo.3/bar.nii.gz').groups()
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
