import os, tempfile
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces import fsl
from nipype.interfaces.dcmstack import MergeNifti
from ..helpers.project import project
from ..helpers import xnat_helper
from ..interfaces import (Unpack, XNATUpload, MriVolCluster)
from .workflow_base import WorkflowBase

import logging
logger = logging.getLogger(__name__)

MASK_RECON = 'mask'
"""The XNAT mask reconstruction name."""


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.mask.MaskWorkflow` and runs it
    on the given inputs.
    
    :param inputs: the :meth:`qipipe.pipeline.mask.MaskWorkflow.run`
        inputs
    :param opts: the :class:`qipipe.pipeline.mask.MaskWorkflow`
        initializer options
    :return: the :meth:`qipipe.pipeline.mask.MaskWorkflow.run`
        result
    """
    return MaskWorkflow(**opts).run(*inputs)


class MaskWorkflow(WorkflowBase):
    """
    The MaskWorkflow class builds and executes the mask workflow.
    
    The workflow creates a mask to subtract extraneous tissue for a given set
    of input session images. The new mask is uploaded to XNAT as a resource of
    the `subject`, `session` and the reconstruction named ``mask``.
    
    The mask workflow input is the `input_spec` node consisting of
    the following input fields:

     - subject: the XNAT subject name

     - session: the XNAT session name

     - images: the image files to mask
    
    The mask workflow output is the `output_spec` node consisting of the
    following output fields:
    
    - `out_file`: the mask file
    
    The optional workflow configuration file can contain the following
    sections:
    
    - ``FSLMriVolCluster``: the
        :class:`qipipe.interfaces.mri_volcluster.MriVolCluster`
        interface options
    
    The execution mask workflow fronts the reusable workflow with an iterable
    input node named ``input_spec`` consisting of the following input fields:
    
    - `session_spec`: the (subject, session, images) tuple to mask
    """
    
    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.
        
        :keyword base_dir: the workflow execution directory (default is a new
            temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        """
        super(MaskWorkflow, self).__init__(logger, opts.pop('cfg_file', None))
        
        self.workflow = self._create_workflow(**opts)
        """The mask creation workflow."""
    
    def run(self, *inputs):
        """
        Runs the mask workflow on the scan NiFTI files for the given
        (subject, session) inputs.
        
        :param inputs: the (subject, session) name tuples to mask
        :return: the mask reconstruction name
        """
        # Set the workflow inputs.
        exec_wf = self.workflow
        dest = os.path.join(exec_wf.base_dir, 'data')
        with xnat_helper.connection() as xnat:
            dl_inputs = [(sbj, sess, self._download_scans(xnat, sbj, sess, dest))
                for sbj, sess in inputs]
        input_spec = exec_wf.get_node('input_spec')
        in_fields = ['subject' 'session', 'images']
        input_spec.synchronize = True

        # Execute the workflow
        self._run_workflow(exec_wf)
        
        return MASK_RECON        
    
    def _create_workflow(self, base_dir=None):
        """
        Creates the mask workflow.
        
        :param base_dir: the workflow execution directory
        :return: the Workflow object
        """
        logger.debug('Creating the mask reusable workflow...')
        
        if not base_dir:
            base_dir = tempfile.mkdtemp()
        workflow = pe.Workflow(name='mask', base_dir=base_dir)
        
        # The workflow input.
        in_fields = ['subject', 'session', 'images']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
            name='input_spec')
        
        # Merge the DCE data to 4D.
        dce_merge = pe.Node(MergeNifti(out_format='dce_series'),
            name='dce_merge')
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
        workflow.connect(find_cog, ('out_stat', _gen_crop_op_string),
            crop_back, 'op_string')
        
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
        inv_mask = pe.Node(fsl.maths.MathsCommand(args='-sub 1 -mul -1'),
            name='inv_mask')
        workflow.connect(binarize, 'out_file', inv_mask, 'in_file')
        workflow.connect(mask_name, 'out_file', inv_mask, 'out_file')
        
        # Upload the mask to XNAT.
        upload_mask = pe.Node(XNATUpload(project=project(),
            reconstruction=MASK_RECON, format='NIFTI'), name='upload_mask')
        workflow.connect(input_spec, 'subject', upload_mask, 'subject')
        workflow.connect(input_spec, 'session', upload_mask, 'session')
        workflow.connect(inv_mask, 'out_file', upload_mask, 'in_files')
        
        # Collect the outputs.
        out_fields = ['out_file']
        output_spec = pe.Node(IdentityInterface(fields=out_fields),
            name='output_spec')
        workflow.connect(inv_mask, 'out_file', output_spec, 'out_file')
        
        return workflow


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