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
from .distributable import DISTRIBUTABLE

import logging
logger = logging.getLogger(__name__)

DEF_CONFIG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'conf', 'registration.cfg')

REG_PREFIX = 'reg'
"""The XNAT registration reconstruction name prefix."""

def run(*inputs, **opts):
    """
    Creates a :class:`RegistrationPipeline` and runs its on the given inputs.
    
    :param inputs: the :meth:`RegistrationPipeline.run` inputs
    :param opts: the :class:`RegistrationPipeline` initializer options
    :return: the :meth:`RegistrationPipeline.run` result
    """
    return RegistrationPipeline(**opts).run(*inputs)

class RegistrationPipeline(object):
    """
    The RegistrationPipeline class builds and executes the registration workflow.
    
    The workflow registers the images as follows:
    
    - Download the NiFTI scans from XNAT
    
    - Make a mask to subtract extraneous tissue
    
    - Mask each input scan
    
    - Make a template by averaging the masked images
    
    - Create an affine and non-rigid transform for each image
    
    - Reslice the masked image with the transforms
    
    - Upload the mask and the resliced images
    
    The NiFTI scan images for each session are downloaded from XNAT into the
    ``scans`` subdirectory of the ``base_dir`` specified in the initializer
    options (default is the current directory).
    
    The average is taken on the middle half of the NiFTI scan images.
    These images are averaged into a fixed reference template image.
    
    The optional workflow inputs configuration file can contain the
    following sections:
    
    - ``FSLMriVolCluster``: the :class:`qipipe.interfaces.mri_volcluster.MriVolCluster` interface options
    
    - ``ANTSAverage``: the ANTS Average_ interface options
    
    - ``ANTSRegistration``: the ANTS Registration_ interface options
    
    - ``ANTSApplyTransforms``: the ANTS ApplyTransform_ interface options
    
    - ``FSLFNIRT``: the FSL FNIRT_ interface options
    
    .. _Average: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.utils.html
    .. _Registration: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.registration.html
    .. _ApplyTransform: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.resampling.html
    .. _FNIRT: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.fsl.preprocess.html
    
    The default registration applies an affine followed by a symmetric normalization
    transform.
    """
    
    def __init__(self, base_dir=None, cfg_file=None):
        """
        If the optional configuration file is specfied, then the workflow settings in
        that file override the default settings.
        
        :keyword base_dir: the workflow execution directory (default current directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        """
        self.config = self._load_configuration(cfg_file)
        """The registration configuration."""
        
        self.registration_workflow = self._create_registration_workflow(base_dir=base_dir)
        """The reusable registration workflow."""
        
        self.mask_workflow = self._create_mask_workflow(base_dir=base_dir)
        """The reusable mask workflow."""
    
    def run(self, *inputs):
        """
        Runs the registration workflow on the given inputs.
        
        The NiFTI scan images for each session are downloaded from XNAT into the
        ``scans`` subdirectory of the ``base_dir`` specified in the
        :class:`RegistrationPipeline` initializer options (default is the current
        directory). The workflow is run on these images, resulting in a new XNAT
        reconstruction object for each session which contains the resliced images.
        
        :param inputs: the (subject, session) name tuples to register
        :return: the registration reconstruction name, unqualified by the
            session parent label
        """
        # Make a unique registration reconstruction name. This permits more than one
        # registration to be stored for each input image without a name conflict.
        recon = "%s_%s" % (REG_PREFIX, file_helper.generate_file_name())
        
        # Run the workflow on each session.
        for sbj, sess in inputs:
            self._register(sbj, sess, recon)
        
        return recon
    
    def _register(self, subject, session, recon):
        """
        Builds and runs a registration execution workflow on each of the given sessions.
        
        :return: the resliced XNAT (subject, session, reconstruction) designator tuple
        """
        # The scan series stack download location.
        dest = os.path.join(self.registration_workflow.base_dir, 'data', subject, session)
        
        # Download the scan images. This step cannot be done in the workflow, since
        # Nipype requires that the workflow iterator is bound to the images when the
        # workflow is built rather than set dynamically by a download step.
        images = self._download_scans(subject, session, dest)
        
        # Make the workflow
        exec_wf = self._create_execution_workflow(subject, session, recon, images)
        
        # Check whether the workflow can be distributed.
        if DISTRIBUTABLE:
            if 'execution' in self.config:
                exec_wf.config['execution'] = self.config['execution']
            if 'SGE' in self.config:
                args = dict(plugin='SGE', plugin_args=self.config['SGE'])
        else:
            args = {}
        
        # Execute the workflow.
        self._run_workflow(exec_wf, **args)
        
        # Return the recon specification.
        return (subject, session, recon)
    
    def _create_execution_workflow(self, subject, session, recon, images):
        """
        Creates the execution workflow. The execution workflow sets the registration workflow
        inputs and uploads the registration workflow outputs to XNAT.
        
        :Note: the execution workflow cannot be embedded in another workflow, since the
            workflow iterates over each image. A Nipype iterator is defined when the
            workflow is built, and cannot be set dynamically during execution.
            Consequently, the subject, session and images inputs are wired into the workflow
            and cannot be set dynamically from the output of a parent workflow node.
        
        :param subject: the subject name
        :param session: the session name
        :param recon: the reconstruction name
        :param images: the scan images to register
        :return: the Workflow object
        """
        logger.debug("Creating the %s %s registration execution workflow" % (subject, session))

        # The registration workflow.
        reg_wf = self.registration_workflow
        
        # The execution workflow.
        wf_name = 'exec_' + reg_wf.name
        exec_wf = pe.Workflow(name=wf_name, base_dir=reg_wf.base_dir)
        
        # The execution workflow input.
        in_fields = ['subject', 'session', 'images', 'reconstruction']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.images = images
        input_spec.inputs.reconstruction = recon
        
        # The registration image iterator.
        in_fields = ['image']
        abs_images = [os.path.abspath(fname) for fname in images]
        image_iter = pe.Node(IdentityInterface(fields=['image']), name='image_iter')
        image_iter.iterables = dict(image=abs_images).items()
        
        # Make the mask.
        mask_wf = self.mask_workflow
        exec_wf.connect(input_spec, 'subject', mask_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', mask_wf, 'input_spec.session')
        exec_wf.connect(input_spec, 'images', mask_wf, 'input_spec.images')
        
        # Upload the mask to XNAT.
        upload_mask = pe.Node(XNATUpload(project=project(), reconstruction='mask', format='NIFTI'),
            name='upload_mask')
        exec_wf.connect(input_spec, 'subject', upload_mask, 'subject')
        exec_wf.connect(input_spec, 'session', upload_mask, 'session')
        exec_wf.connect(mask_wf, 'output_spec.mask', upload_mask, 'in_files')
        
        # The average options.
        avg_opts = self.config.get('ANTSAverage', {})
        # Make the ANTS template.
        average = pe.Node(AverageImages(**avg_opts), name='average')
        # Use the middle half of the images.
        offset = len(images) / 4
        average.inputs.images = sorted(images)[offset:len(images)-offset]
        
        # Register the images.
        exec_wf.connect(input_spec, 'subject', reg_wf, 'input_spec.subject')
        exec_wf.connect(input_spec, 'session', reg_wf, 'input_spec.session')
        exec_wf.connect(input_spec, 'reconstruction', reg_wf, 'input_spec.reconstruction')
        exec_wf.connect(image_iter, 'image', reg_wf, 'input_spec.moving_image')
        exec_wf.connect(mask_wf, 'output_spec.mask', reg_wf, 'input_spec.mask')
        exec_wf.connect(average, 'output_average_image', reg_wf, 'input_spec.fixed_image')
        
        # Upload the resliced image to XNAT.
        upload_reg = pe.Node(XNATUpload(project=project(), reconstruction=recon, format='NIFTI'),
            name='upload_reg')
        exec_wf.connect(input_spec, 'subject', upload_reg, 'subject')
        exec_wf.connect(input_spec, 'session', upload_reg, 'session')
        exec_wf.connect(reg_wf, 'output_spec.resliced', upload_reg, 'in_files')
        
        return exec_wf
    
    def _create_mask_workflow(self, **opts):
        """
        Creates the mask workflow for the given images.
        
        :param subject: the subject name
        :param session: the session name
        :param images: the input images
        :param opts: the workflow options
        :return: the Workflow object
        """
        logger.debug('Creating the mask workflow...')
        
        workflow = pe.Workflow(name='mask', **opts)
        
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
        mask_opts = self.config.get('FSLMriVolCluster', {})
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
        mask_name= pe.Node(mask_name_func, name='mask_name')
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
    
    def _create_registration_workflow(self, base_dir=None, technique='ANTS'):
        """
        Creates the reusable workflow which registers images.
        
        :param base_dir: the workflow execution directory (default is the current directory)
        :param technique: the registration technique (``ANTS`` or ``FNIRT``)
        :return: the Workflow object
        """
        logger.debug('Creating the registration reusable workflow...')
        
        if base_dir:
            base_dir = os.path.abspath(base_dir)
        else:
            base_dir = os.getcwd()
        
        workflow = pe.Workflow(name='register', base_dir=base_dir)
        
        # The workflow input image iterator.
        in_fields = ['subject', 'session', 'mask', 'fixed_image', 'moving_image', 'reconstruction']
        input_spec = pe.Node(IdentityInterface(fields=in_fields), name='input_spec')
        
        # Make the resliced image file name.
        reslice_name_func = Function(input_names=['recon', 'in_file'],
            output_names=['out_file'],
            function=_gen_reslice_filename)
        reslice_name = pe.Node(reslice_name_func, name='reslice_name')
        workflow.connect(input_spec, 'reconstruction', reslice_name, 'recon')
        workflow.connect(input_spec, 'moving_image', reslice_name, 'in_file')
        
        if technique == 'ANTS':
            # The ANTS registration options.
            reg_opts = self.config.get('ANTSRegistration', {})
            # Register the images to create the warp and affine transformations.
            register = pe.Node(Registration(**reg_opts), name='register')
            workflow.connect(input_spec, 'fixed_image', register, 'fixed_image')
            workflow.connect(input_spec, 'moving_image', register, 'moving_image')
            workflow.connect(input_spec, 'mask', register, 'fixed_image_mask')
            workflow.connect(input_spec, 'mask', register, 'moving_image_mask')
            # The ANTS reslice options.
            reslice_opts = self.config.get('ANTSApplyTransforms', {})
            # Apply the transforms to the input image.
            reslice = pe.Node(ApplyTransforms(**reslice_opts), name='reslice')
            workflow.connect(input_spec, 'fixed_image', reslice, 'reference_image')
            workflow.connect(input_spec, 'moving_image', reslice, 'input_image')
            workflow.connect(reslice_name, 'out_file', reslice, 'output_image')
            workflow.connect(register, 'forward_transforms', reslice, 'transforms')
        elif technique == 'FNIRT':
            # The FNIRT registration options.
            reg_opts = self.config.get('FSLFNIRT', {})
            # Register the images to create the warp and affine transformations.
            reslice = pe.Node(Registration(**reg_opts), name='reslice')
            workflow.connect(input_spec, 'fixed_image', reslice, 'fixed_image')
            workflow.connect(input_spec, 'moving_image', reslice, 'moving_image')
            workflow.connect(input_spec, 'mask', reslice, 'fixed_image_mask')
            workflow.connect(input_spec, 'mask', reslice, 'moving_image_mask')
        else:
            raise PipelineError("Registration technique not recognized: %s" % technique)
        
        # Copy the DICOM meta-data.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        workflow.connect(input_spec, 'moving_image', copy_meta, 'src_file')
        workflow.connect(reslice, 'output_image', copy_meta, 'dest_file')
        
        # Collect the outputs.
        out_fields = ['resliced']
        output_spec = pe.Node(IdentityInterface(fields=out_fields), name='output_spec')
        workflow.connect(copy_meta, 'dest_file', output_spec, 'resliced')
        
        return workflow
    
    def _load_configuration(self, cfg_file=None):
        """
        Loads the registration workflow configuration.
        
        :param cfg_file: the optional configuration file path
        :return: the configuration dictionary
        """
        if cfg_file:
            cfg = read_config(DEF_CONFIG_FILE, cfg_file)
        else:
            cfg = read_config(DEF_CONFIG_FILE)
        return dict(cfg)
    
    def _download_scans(self, subject, session, dest):
        """
        Download the NIFTI scan files for the given session.
        
        :param subject: the XNAT subject label
        :param session: the XNAT session label
        :param dest: the destination directory path
        :return: the download file paths
        """
        with xnat_helper.connection() as xnat:
            return xnat.download(project(), subject, session, dest=dest, container_type='scan', format='NIFTI')
    
    def _run_workflow(self, workflow):
        """
        Executes the given workflow.
        
        If the logger level is set to debug, then workflow graph
        is printed in the workflow base directory.
        
        :param workflow: the workflow to run
        """
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            fname = "%s.dot" % workflow.name
            if workflow.base_dir:
                grf = os.path.join(workflow.base_dir, fname)
            else:
                grf = fname
            workflow.write_graph(dotfilename=grf)
            logger.debug("The %s workflow graph is depicted at %s.png." % (workflow.name, grf))
        
        # Check whether the workflow can be distributed.
        if DISTRIBUTABLE:
            exec_wf.config['execution'] = {'job_finished_timeout': 60.0}
            args = dict(plugin='SGE',
                        plugin_args={'qsub_args' : '-l h_rt=1:00:00,mf=3G,h_vmem=3.5G -b n'})
        else:
            args = {}
        
        # Run the workflow.
        with xnat_helper.connection():
            workflow.run()

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

def _gen_reslice_filename(recon, in_file):
    """
    :param recon: the reconstruction name
    :param in_file: the input scan image filename
    :return: the registered image filename
    """
    from qipipe.pipelines.registration import SPLITEXT_PAT
    
    groups = SPLITEXT_PAT.match(in_file).groups()
    base = groups[0]
    fname = "%s_%s" % (base, recon)
    if len(groups) == 1:
        return fname
    else:
        ext = groups[1]
        return fname + ext

def _gen_mask_filename(subject, session):
    return "%s_%s_mask.nii.gz" % (subject.lower(), session.lower())

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
