import os, re
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.ants.registration import Registration
from nipype.interfaces.ants import AverageImages, ApplyTransforms
from nipype.interfaces.dcmstack import CopyMeta
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import DcmStack, MergeNifti, CopyMeta
from ..helpers.project import project
from ..interfaces import XNATDownload, XNATUpload, MriVolCluster
from ..helpers import xnat_helper, file_helper
from ..helpers.ast_config import read_config
from .workflow_base import WorkflowBase
from .distributable import DISTRIBUTABLE

import logging
logger = logging.getLogger(__name__)

REG_PREFIX = 'reg'
"""The XNAT registration reconstruction name prefix."""

MASK_RECON = 'mask'
"""The XNAT mask reconstruction name."""

def run(*inputs, **opts):
    """
    Creates a :class:`RegistrationWorkflow` and runs its on the given inputs.
    
    :param inputs: the :meth:`RegistrationWorkflow.run` inputs
    :param opts: the :class:`RegistrationWorkflow` initializer options
    :return: the :meth:`RegistrationWorkflow.run` result
    """
    return RegistrationWorkflow(**opts).run(*inputs)

class RegistrationWorkflow(WorkflowBase):
    """
    The RegistrationWorkflow class builds and executes the registration workflow.
    
    The workflow registers the images as follows:
    
    - Obtain a mask to subtract extraneous tissue
    
    - Make a fixed reference image
    
    - Register each image against the reference image
    
    - Upload the registered images
    
    The registration workflow input is the ``input_spec`` node consisting of the
    following input fields:
    
    - ``subject``: the subject name
    
    - ``session``: the session name
    
    - ``recon``: the registration XNAT reconstruction name
    
    - ``images``: the session images to register
    
    In addition, the iterable ``image_iter`` node input field ``images`` must
    be set to the input session images.
    
    The fixed reference image is the average of the middle three input images.
    
    Two registration techniques are supported:
    
    - ANTS_ SyN_ symmetric normalization diffeomorphic registration (default)
    
    - FSL_ FNIRT_ non-linear registration
    
    The optional workflow configuration file can contain the following sections:
    
    - ``FSLMriVolCluster``: the :class:`qipipe.interfaces.mri_volcluster.MriVolCluster`
        interface options
    
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
        
        :param opts: the following options
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword base_dir: the workflow execution directory (default current directory)
        :keyword technique: the case-insensitive workflow technique
            (``ANTS`` or ``FNIRT``, default ``ANTS``)
        """
        super(RegistrationWorkflow, self).__init__(logger, opts.pop('cfg_file', None))
        
        self._reg_mask_dl_wf = self._create_workflow_with_existing_mask(**opts)
        """The registration workflow to use with a existing mask."""
        
        self._reg_mask_cr_wf = self._create_workflow_with_nonexisting_mask(**opts)
        """The registration workflow to use with a non-existing mask."""
    
    def run(self, *inputs):
        """
        Runs the registration workflow on the given inputs.
        
        The NiFTI scan images for each session are downloaded from XNAT into the
        ``scans`` subdirectory of the ``base_dir`` specified in the
        :class:`RegistrationWorkflow` initializer options (default is the current
        directory). The workflow is run on these images, resulting in a new XNAT
        reconstruction object for each session which contains the resliced images.
        
        :param inputs: the (subject, session) name tuples to register
        :return: the registration reconstruction name, unqualified by the
            session parent label
        """
        recon = self.generate_reconstruction_name()
        
        # Run the workflow on each session.
        for sbj, sess in inputs:
            self._register(sbj, sess, recon)
        
        return recon
    
    def generate_reconstruction_name(self):
        """
        Makes a unique registration reconstruction name for the registration
        workflow input. Uniqueness permits more than one registration to be
        stored without a name conflict.
        
        :return: a unique XNAT registration reconstruction name
        """
        return "%s_%s" % (REG_PREFIX, file_helper.generate_file_name())
    
    def _register(self, subject, session, recon):
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
        images = self._download_scans(subject, session, dest)
        # Sort the images by series number.
        images.sort()
        
        # The workflow to use depends on whether there is a mask.
        reg_wf = self._get_workflow(subject, session)
        
        # Execute the registration workflow.
        self._set_registration_input(reg_wf, subject, session, recon, images)
        logger.debug("Executing the %s workflow on %s %s..." %
            (reg_wf.name, subject, session))
        self._run_workflow(reg_wf)
        logger.debug("%s %s is registered as reconstruction %s." %
            (subject, session, recon))
    
    def _get_workflow(self, subject, session):
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
    
    def _set_registration_input(self, workflow, subject, session, recon, images):
        """
        Sets the registration input.
        
        :param workflow: the registration workflow
        :param subject: the subject name
        :param session: the session name
        :param recon: the reconstruction name
        :param images: the scan images to register
        """
        # Set the workflow inputs.
        input_spec = workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.reconstruction = recon
        input_spec.inputs.images = images
        
        # The images are iterable in the reslice workflow.
        image_iter = workflow.get_node('image_iter')
        abs_images = [os.path.abspath(fname) for fname in images]
        image_iter.iterables = dict(image=abs_images).items()
    
    def _create_workflow_with_existing_mask(self, **opts):
        logger.debug("Creating the registration workflow to use with a existing mask...")
        
        # Start with a base registration workflow.
        reg_wf = self._create_base_workflow(name='reg_existing_mask',**opts)
        
        # Download the mask.
        input_spec = reg_wf.get_node('input_spec')
        dl_mask = pe.Node(XNATDownload(project=project(), reconstruction=MASK_RECON),
            name='dl_mask')
        reg_wf.connect(input_spec, 'subject', dl_mask, 'subject')
        reg_wf.connect(input_spec, 'session', dl_mask, 'session')
        
        # Connect the mask download output to the registration mask input.
        mask = reg_wf.get_node('mask')
        reg_wf.connect(dl_mask, 'out_file', mask, 'mask')
        
        logger.debug("Created the %s workflow." % reg_wf.name)
        
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(reg_wf)
        
        return reg_wf
    
    def _create_workflow_with_nonexisting_mask(self, **opts):
        logger.debug("Creating the registration workflow to use with a non-existing mask...")
        
        # The mask creation workflow.
        mask_wf = self._create_mask_workflow(base_dir=opts.get('base_dir'))
        
        # Start with a base registration workflow.
        reg_wf = self._create_base_workflow(name='reg_nonexisting_mask', **opts)
        
        # Connect the mask workflow inputs.
        input_spec = reg_wf.get_node('input_spec')
        reg_wf.connect(input_spec, 'subject', mask_wf, 'input_spec.subject')
        reg_wf.connect(input_spec, 'session', mask_wf, 'input_spec.session')
        reg_wf.connect(input_spec, 'images', mask_wf, 'input_spec.images')
        
        # Upload the mask to XNAT.
        upload_mask = pe.Node(XNATUpload(project=project(), reconstruction=MASK_RECON,
            format='NIFTI'), name='upload_mask')
        reg_wf.connect(input_spec, 'subject', upload_mask, 'subject')
        reg_wf.connect(input_spec, 'session', upload_mask, 'session')
        reg_wf.connect(mask_wf, 'output_spec.mask', upload_mask, 'in_files')
        
        # Connect the mask workflow output to the registration mask input.
        mask = reg_wf.get_node('mask')
        reg_wf.connect(mask_wf, 'output_spec.mask', mask, 'mask')
        
        logger.debug("Created the %s workflow." % reg_wf.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(reg_wf)
        
        return reg_wf
    
    def _create_base_workflow(self, name, **opts):
        """
        Creates the base registration workflow. The registration workflow performs the
        following steps:
        
        - Set the mask and reslice workflow inputs
        
        - Run these workflows
        
        - Upload the mask and reslice outputs to XNAT
        
        :param name: the workflow name
        :param opts: the following workflow options
        :keyword base_dir: the workflow execution directory (default current directory)
        :keyword technique: the registration technique
            ('``ANTS`` or ``FNIRT``, default ``ANTS``)
        :return: the Workflow object
        """
        logger.debug("Creating a base registration workflow...")
        
        # The execution workflow.
        base_wf = pe.Workflow(name=name, base_dir=opts.get('base_dir'))
        
        # The execution workflow input.
        in_fields = ['subject', 'session', 'reconstruction', 'images']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # The mask is set by the execution workflow.
        mask = pe.Node(IdentityInterface(fields=['mask']), name='mask')
        
        # The average options.
        avg_opts = self.configuration.get('ANTSAverage', {})
        # Make the reference image.
        average = pe.Node(AverageImages(**avg_opts), name='average')
        # The average is taken over the middle three images.
        base_wf.connect(input_spec, ('images', _middle, 3), average, 'images')
        
        # Mask the reference image.
        mask_ref = pe.Node(fsl.maths.ApplyMask(output_type='NIFTI_GZ'), name='mask_ref')
        base_wf.connect(average, 'output_average_image', mask_ref, 'in_file')
        base_wf.connect(mask, 'mask', mask_ref, 'mask_file')
        
        # The reslice image iterator.
        image_iter = pe.Node(IdentityInterface(fields=['image']), name='image_iter')
        
        # The reslice workflow.
        reslice_wf = self._create_reslice_workflow(**opts)
        
        # Register and resample the images.
        base_wf.connect(input_spec, 'subject', reslice_wf, 'input_spec.subject')
        base_wf.connect(input_spec, 'session', reslice_wf, 'input_spec.session')
        base_wf.connect(input_spec, 'reconstruction', reslice_wf, 'input_spec.reconstruction')
        base_wf.connect(image_iter, 'image', reslice_wf, 'input_spec.moving_image')
        base_wf.connect(mask_ref, 'out_file', reslice_wf, 'input_spec.mask')
        base_wf.connect(average, 'output_average_image', reslice_wf, 'input_spec.fixed_image')
        
        # Upload the resliced image to XNAT.
        upload_reg = pe.Node(XNATUpload(project=project(), format='NIFTI'),
            name='upload_reg')
        base_wf.connect(input_spec, 'subject', upload_reg, 'subject')
        base_wf.connect(input_spec, 'session', upload_reg, 'session')
        base_wf.connect(input_spec, 'reconstruction', upload_reg, 'reconstruction')
        base_wf.connect(reslice_wf, 'output_spec.resliced', upload_reg, 'in_files')
        
        # The workflow output is the resliced images.
        reslice_output = reslice_wf.get_node('output_spec')
        out_fields = reslice_output.outputs.copyable_trait_names()
        output_spec = pe.Node(IdentityInterface(fields=out_fields), name='output_spec')
        for field in out_fields:
            reslice_field = 'output_spec.' + field
            base_wf.connect(reslice_wf, reslice_field, output_spec, field)
        
        return base_wf
    
    def _create_mask_workflow(self, base_dir=None):
        """
        Creates the mask workflow.
        
        :param base_dir: the workflow execution directory
        :return: the Workflow object
        """
        logger.debug('Creating the mask workflow...')
        
        workflow = pe.Workflow(name='make_mask', base_dir=base_dir)
        
        # The workflow inputs.
        in_fields = ['subject', 'session', 'images']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # Merge the DCE data to 4D.
        dce_merge = pe.Node(MergeNifti(out_format='dce_series'), name='dce_merge')
        workflow.connect(input_spec, 'images', dce_merge, 'in_files')
        
        # Get a mean image from the DCE data.
        dce_mean = pe.Node(fsl.MeanImage(), name='dce_mean')
        workflow.connect(dce_merge, 'out_file', dce_mean, 'in_file')
        
        # Find the center of gravity from the mean image.
        find_cog = pe.Node(fsl.ImageStats(), name='find_cog')
        find_cog.inputs.op_string = '-C'
        workflow.connect(dce_mean, 'out_file', find_cog, 'in_file')
        
        # Zero everything posterior to the center of gravity on mean image.
        crop_back = pe.Node(fsl.ImageMaths(), name='crop_back')
        workflow.connect(dce_mean, 'out_file', crop_back, 'in_file')
        workflow.connect(find_cog, ('out_stat', _gen_crop_op_string), crop_back, 'op_string')
        
        # The cluster options.
        mask_opts = self.configuration.get('FSLMriVolCluster', {})
        # Find large clusters of empty space on the cropped image.
        cluster_mask = pe.Node(MriVolCluster(**mask_opts), name='cluster_mask')
        workflow.connect(crop_back, 'out_file', cluster_mask, 'in_file')
        
        # Convert the cluster labels to a binary mask.
        binarize = pe.Node(fsl.BinaryMaths(), name='binarize')
        binarize.inputs.operation = 'min'
        binarize.inputs.operand_value = 1
        workflow.connect(cluster_mask, 'out_cluster_file', binarize, 'in_file')
        
        # Make the mask file name.
        mask_name_func = Function(input_names=['subject', 'session'],
            output_names=['out_file'],
            function=_gen_mask_filename)
        mask_name = pe.Node(mask_name_func, name='mask_name')
        workflow.connect(input_spec, 'subject', mask_name, 'subject')
        workflow.connect(input_spec, 'session', mask_name, 'session')
        
        # Invert the binary mask.
        inv_mask = pe.Node(fsl.maths.MathsCommand(), name='inv_mask')
        inv_mask.inputs.args = '-sub 1 -mul -1'
        workflow.connect(mask_name, 'out_file', inv_mask, 'out_file')
        workflow.connect(binarize, 'out_file', inv_mask, 'in_file')
        
        # Collect the outputs.
        out_fields = ['mask']
        output_spec = pe.Node(IdentityInterface(fields=out_fields), name='output_spec')
        workflow.connect(inv_mask, 'out_file', output_spec, 'mask')
        
        return workflow
    
    def _create_reslice_workflow(self, base_dir=None, technique='ANTS'):
        """
        Creates the workflow which registers and resamples images.
        
        :param base_dir: the workflow execution directory (default is the current directory)
        :param technique: the registration technique (``ANTS`` or ``FNIRT``)
        :return: the Workflow object
        """
        logger.debug('Creating the reslice workflow...')
        
        workflow = pe.Workflow(name='reslice', base_dir=base_dir)
        
        # The workflow input image iterator.
        in_fields = ['subject', 'session', 'mask', 'fixed_image', 'moving_image', 'reconstruction']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # Make the resliced image file name.
        reslice_name_func = Function(input_names=['reconstruction', 'in_file'],
            output_names=['out_file'],
            function=_gen_reslice_filename)
        reslice_name = pe.Node(reslice_name_func, name='reslice_name')
        workflow.connect(input_spec, 'reconstruction', reslice_name, 'reconstruction')
        workflow.connect(input_spec, 'moving_image', reslice_name, 'in_file')
        
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
            # The ANTS reslice options.
            apply_opts = self.configuration.get('ANTSApplyTransforms', {})
            # Apply the transforms to the input image.
            apply_xfm = pe.Node(ApplyTransforms(**apply_opts), name='apply_xfm')
            workflow.connect(input_spec, 'fixed_image', apply_xfm, 'reference_image')
            workflow.connect(input_spec, 'moving_image', apply_xfm, 'input_image')
            workflow.connect(reslice_name, 'out_file', apply_xfm, 'output_image')
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
            workflow.connect(reslice_name, 'out_file', fnirt, 'warped_file')
            # Copy the meta-data.
            workflow.connect(fnirt, 'warped_file', copy_meta, 'dest_file')
        else:
            raise PipelineError("Registration technique not recognized: %s" % technique)
        
        # The output is the resliced images.
        out_fields = ['resliced']
        output_spec = pe.Node(IdentityInterface(fields=out_fields), name='output_spec')
        workflow.connect(copy_meta, 'dest_file', output_spec, 'resliced')
        
        return workflow
    
    def _download_scans(self, subject, session, dest):
        """
        Download the NIFTI scan files for the given session.
        
        :param subject: the XNAT subject label
        :param session: the XNAT session label
        :param dest: the destination directory path
        :return: the download file paths
        """
        with xnat_helper.connection() as xnat:
            return xnat.download(project(), subject, session, dest=dest,
                container_type='scan', format='NIFTI')


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

def _gen_reslice_filename(reconstruction, in_file):
    """
    :param reconstruction: the reconstruction name
    :param in_file: the input scan image filename
    :return: the registered image filename
    """
    from qipipe.pipelines.registration import SPLITEXT_PAT
    
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

def _gen_crop_op_string(cog):
    """
    :param cog: the center of gravity
    :return: the crop -roi option
    """
    return "-roi 0 -1 %d -1 0 -1 0 -1" % cog[1]

def _crop_posterior(image, cog):
    from nipype.interfaces import fsl
    
    crop_back = fsl.ImageMaths()
    crop_back.inputs.op_string = '-roi 0 -1 %d -1 0 -1 0 -1' % cog[1]
    crop_back.inputs.in_file = image
    return crop_back.run().outputs.out_file
