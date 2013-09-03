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
from ..helpers.project import project
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
    The workflow registers the images as follows:
    
    - Make a fixed reference image
    
    - Register each image against the reference image
    
    - Upload the registered images
    
    The registration workflow input is the ``input_spec`` node consisting of the
    following input fields:
    
    - ``subject``: the subject name
    
    - ``session``: the session name
    
    - ``mask``: the mask to apply to the images
    
    - ``images``: the session images to register
    
    In addition, the iterable ``iter_image`` node input field ``image`` must
    be set to the input session images.
    
    The mask can be obtained by running the
    :class:`qipipe.pipeline.mask.MaskWorkflow` workflow.
    
    The registration workflow output is the ``output_spec`` node consisting of
    the following output fields:
    
    - ``images``: the realigned image files
    
    In addition, the workflow implements an ``iter_realigned`` output node
    consisting of the following output fields:
    
    - ``image``: the realigned image file for the iterated input image file
    
    There are two registration workflow paths:
    
    - a path which uses the images list as an input to create the mask and
      registration reference image
    
    - a path which uses each image as an input to realign the image
    
    The image realign path uses outputs from the mask/reference path.
    
    The fixed reference image is the average of the middle three input images.
    
    Two registration techniques are supported:
    
    - ANTS_ SyN_ symmetric normalization diffeomorphic registration (default)
    
    - FSL_ FNIRT_ non-linear registration
    
    The optional workflow configuration file can contain overrides for the
    Nipype interface inputs in the following sections:
    
    - ``ants.AverageImages``: the ANTS `Average interface`_ options
    
    - ``ants.Registration``: the ANTS `Registration interface`_ options
    
    - ``ants.ApplyTransforms``: the ANTS `ApplyTransform interface`_ options
    
    - ``fsl.FNIRT``: the FSL `FNIRT interface`_ options
    
    :Note: Since the XNAT ``reconstruction`` name is unique, a
        :class:`qipipe.pipeline.registration.RegistrationWorkflow` instance
        can be used for only one registration workflow. Different registration
        inputs require different
        :class:`qipipe.pipeline.registration.RegistrationWorkflow` instances.
    
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

        self.reconstruction = opts.pop('reconstruction',
                                       self._generate_reconstruction_name())
        """The XNAT reconstruction name used for all runs against this workflow
        instance."""

        self.workflow = self._create_workflow(**opts)
        """The registration workflow."""

    def run(self, input_dict):
        """
        Runs the registration workflow on the given inputs.
        
        The NiFTI scan images for each session are downloaded from XNAT into the
        ``scans`` subdirectory of the ``base_dir`` specified in the
        :class:`RegistrationWorkflow` initializer options (default is the current
        directory). The workflow is run on these images, resulting in a new XNAT
        reconstruction object for each session which contains the realigned images.
        
        :param input_dict: the input *{subject: {session: ([images], mask)}}* to
            register
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
        self.logger.debug("Registering %d images in %d sessions..." %
                         (img_cnt, sess_cnt))
        for sbj, sess_dict in input_dict.iteritems():
            self.logger.debug("Registering subject %s..." % sbj)
            for sess, sess_inputs in sess_dict.iteritems():
                images, mask = sess_inputs
                self.logger.debug("Registering %d %s %s images" %
                                 (len(images), sbj, sess))
                self._register(sbj, sess, images, mask)
                self.logger.debug("Registered %s %s." % (sbj, sess))
            self.logger.debug("Registered subject %s." % sbj)
        self.logger.debug("Registered %d images from %d sessions." %
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

    def _register(self, subject, session, images, mask):
        """
        Runs the registration workflow on the given session images.
        
        :param subject: the subject name
        :param session: the session name
        :param images: the input session images
        :param mask: the image mask
        """
        # Sort the images by series number, since the fixed reference image
        # is an average of several middle series.
        images = sorted(images)

        # Set the workflow input.
        self._set_registration_input(subject, session, images, mask)
        # Execute the registration workflow.
        self.logger.debug("Executing the %s workflow on %s %s..." %
                         (self.workflow.name, subject, session))
        self._run_workflow(self.workflow)
        self.logger.debug("Executed the %s workflow on %s %s." %
                         (self.workflow.name, subject, session))

    def _set_registration_input(self, subject, session, images, mask):
        """
        Sets the registration input.
        
        :param subject: the subject name
        :param session: the session name
        :param images: the scan images to register
        :param mask: the image mask
        """
        # Validate the mask input.
        if not mask:
            raise ValueError("The mask option is required for registration.")

        # Set the workflow inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.images = images
        input_spec.inputs.mask = mask

        # The images are iterable in the realign workflow.
        iter_image = self.workflow.get_node('iter_image')
        abs_images = [os.path.abspath(fname) for fname in images]
        iter_image.iterables = ('image', abs_images)

    def _create_workflow(self, base_dir=None, technique='ANTS'):
        """
        Creates the base registration workflow. The registration workflow
        performs the following steps:
        
        - Generates a unique XNAT reconstruction name
        
        - Set the mask and realign workflow inputs
        
        - Run these workflows
        
        - Upload the mask and realign outputs to XNAT
        
        :param base_dir: the workflow execution directory
            (default is a new temp directory)
        :param technique: the registration technique
            ('``ANTS`` or ``FNIRT``, default ``ANTS``)
        :return: the Workflow object
        """
        self.logger.debug("Creating a base registration workflow...")

        # The reusable workflow.
        if not base_dir:
            base_dir = tempfile.mkdtemp()
        reg_wf = pe.Workflow(name='registration', base_dir=base_dir)

        # The workflow input.
        in_fields = ['subject', 'session', 'images', 'mask']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')

        # Make the reference image.
        average = pe.Node(AverageImages(), name='average')
        # The average is taken over the middle three images.
        reg_wf.connect(input_spec, ('images', _middle, 3), average, 'images')

        # The realign image iterator.
        iter_image = pe.Node(IdentityInterface(fields=['image']),
                             name='iter_image')

        # The realign workflow.
        realign_wf = self._create_realign_workflow(base_dir, technique)

        # Register and resample the images.
        reg_wf.connect(input_spec, 'subject', realign_wf, 'input_spec.subject')
        reg_wf.connect(input_spec, 'session', realign_wf, 'input_spec.session')
        reg_wf.connect(
            iter_image, 'image', realign_wf, 'input_spec.moving_image')
        reg_wf.connect(input_spec, 'mask', realign_wf, 'input_spec.mask')
        reg_wf.connect(average, 'output_average_image',
                       realign_wf, 'input_spec.fixed_image')

        # Upload the realigned image to XNAT.
        upload_reg_xfc = XNATUpload(project=project(),
                                    reconstruction=self.reconstruction,
                                    format='NIFTI')
        upload_reg = pe.Node(upload_reg_xfc, name='upload_reg')
        reg_wf.connect(input_spec, 'subject', upload_reg, 'subject')
        reg_wf.connect(input_spec, 'session', upload_reg, 'session')
        reg_wf.connect(
            realign_wf, 'output_spec.out_file', upload_reg, 'in_files')

        # The realigned image file iterator.
        iter_realigned = pe.Node(
            IdentityInterface(fields=['image']), name='iter_realigned')
        reg_wf.connect(
            realign_wf, 'output_spec.out_file', iter_realigned, 'image')

        # The workflow output is the realigned image files.
        output_spec = pe.JoinNode(IdentityInterface(fields=['images']),
                                  joinsource='iter_image', joinfield='images',
                                  name='output_spec')
        reg_wf.connect(iter_realigned, 'image', output_spec, 'images')

        self._configure_nodes(reg_wf)

        self.logger.debug("Created the %s workflow." % reg_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self._depict_workflow(reg_wf)

        return reg_wf

    def _create_realign_workflow(self, base_dir, technique='ANTS'):
        """
        Creates the workflow which registers and resamples images.
        
        :param base_dir: the workflow execution directory
        :param technique: the registration technique (``ANTS`` or ``FNIRT``)
        :return: the Workflow object
        """
        self.logger.debug('Creating the realign workflow...')

        realign_wf = pe.Workflow(name='realign', base_dir=base_dir)

        # The workflow input image iterator.
        in_fields = ['subject', 'session', 'mask', 'fixed_image',
                     'moving_image', 'reconstruction']
        input_spec = pe.Node(
            IdentityInterface(fields=in_fields), name='input_spec')
        input_spec.inputs.reconstruction = self.reconstruction

        # Make the realigned image file name.
        realign_name_func = Function(input_names=['reconstruction', 'in_file'],
                                     output_names=['out_file'], function=_gen_realign_filename)
        realign_name = pe.Node(realign_name_func, name='realign_name')
        realign_wf.connect(
            input_spec, 'reconstruction', realign_name, 'reconstruction')
        realign_wf.connect(input_spec, 'moving_image', realign_name, 'in_file')

        # Copy the DICOM meta-data. The copy target is set by the technique
        # node defined below.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        realign_wf.connect(input_spec, 'moving_image', copy_meta, 'src_file')

        if not technique or technique.lower() == 'ants':
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
                input_spec, 'fixed_image', register, 'fixed_image')
            realign_wf.connect(
                input_spec, 'moving_image', register, 'moving_image')
            realign_wf.connect(
                input_spec, 'mask', register, 'fixed_image_mask')
            realign_wf.connect(
                input_spec, 'mask', register, 'moving_image_mask')
            # Apply the transforms to the input image.
            apply_xfm = pe.Node(ApplyTransforms(), name='apply_xfm')
            realign_wf.connect(
                input_spec, 'fixed_image', apply_xfm, 'reference_image')
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
            # Register the images.
            fnirt = pe.Node(fsl.FNIRT(), name='fnirt')
            realign_wf.connect(input_spec, 'fixed_image', fnirt, 'ref_file')
            realign_wf.connect(input_spec, 'moving_image', fnirt, 'in_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'inmask_file')
            realign_wf.connect(input_spec, 'mask', fnirt, 'refmask_file')
            realign_wf.connect(realign_name, 'out_file', fnirt, 'warped_file')
            # Copy the meta-data.
            realign_wf.connect(fnirt, 'warped_file', copy_meta, 'dest_file')
        else:
            raise ValueError("Registration technique not recognized: %s" %
                             technique)

        # The output is the realigned images.
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
        length = min(len(items), proportion_or_length)
        offset = int((len(items) - length) / 2)
    else:
        raise ValueError("The _middle proportion_or_length parameter is not"
                         " a number: %s" % proportion_or_length)

    return sorted(items)[offset:len(items) - offset]
