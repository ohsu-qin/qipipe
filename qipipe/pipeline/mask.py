import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces import fsl
from ..helpers.constants import (SCAN_TS_RSC, MASK_RSC)
from ..interfaces import (XNATUpload, MriVolCluster)
from .workflow_base import WorkflowBase
from qiutil.logging import logger


def run(project, subject, session, scan, time_series, **opts):
    """
    Creates a :class:`qipipe.pipeline.mask.MaskWorkflow` and runs it
    on the given inputs.
    
    :param project: the project name
    :param subject: the input subject
    :param session: the input session
    :param scan: the input scan number
    :param time_series: the input 4D NIfTI time series to mask
    :param opts: additional :class:`MaskWorkflow` initialization parameters
    :return: the XNAT mask resource name
    """
    wf_gen = MaskWorkflow(project=project, **opts)
    
    return wf_gen.run(subject, session, scan, time_series)


class MaskWorkflow(WorkflowBase):
    """
    The MaskWorkflow class builds and executes the mask workflow.
    
    The workflow creates a mask to subtract extraneous tissue for a given
    input session 4D NIfTI time series. The new mask is uploaded to XNAT
    as a session resource named ``mask``.
    
    The mask workflow input is the `input_spec` node consisting of
    the following input fields:
     
     - subject: the XNAT subject name
     
     - session: the XNAT session name
     
     - scan: the XNAT scan number
     
     - time_series: the 4D NIfTI series image file
    
    The mask workflow output is the `output_spec` node consisting of the
    following output field:
    
    - `mask`: the mask file
    
    The optional workflow configuration file can contain the following
    sections:
    
    - ``fsl.MriVolCluster``: the
        :class:`qipipe.interfaces.mri_volcluster.MriVolCluster`
        interface options
    """
    
    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.
        
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer keyword arguments, as well as the following keyword arguments:
        :option crop_posterior: crop posterior to the center of gravity,
            e.g. for a breast tumor
        """
        super(MaskWorkflow, self).__init__(logger=logger(__name__), **opts)
        
        crop_posterior = opts.get('crop_posterior', False)
        self.workflow = self._create_workflow(crop_posterior)
        """The mask creation workflow."""
    
    def run(self, subject, session, scan, time_series):
        """
        Runs the mask workflow on the scan NIfTI files for the given
        time series.
        
        :param subject: the input subject
        :param session: the input session
        :param scan: the input scan number
        :param time_series: the input 3D NIfTI time series to mask
        :return: the mask XNAT resource name
        """
        self.logger.debug("Creating the mask for the %s %s scan %d time series"
                           " %s..." % (subject, session, scan, time_series))
        self.set_inputs(subject, session, scan, time_series)
        # Execute the workflow.
        self._run_workflow(self.workflow)
        self.logger.debug("Created the %s %s scan %s time series %s mask XNAT"
                           " resource %s." %
                           (subject, session, scan, time_series, MASK_RSC))
        
        # Return the mask XNAT resource name.
        return MASK_RSC
    
    def set_inputs(self, subject, session, scan, time_series):
        # Set the inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.time_series = time_series
    
    def _create_workflow(self, crop_posterior=False):
        """
        Creates the mask workflow.
        
        :param crop_posterior: flag indicating whether to crop the
            image posterior in the mask, e.g. for a breast tumor
            (default False)
        :return: the Workflow object
        """
        self.logger.debug('Creating the mask reusable workflow...')
        workflow = pe.Workflow(name='mask', base_dir=self.base_dir)
        
        # The workflow input.
        in_fields = ['subject', 'session', 'scan', 'time_series']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        
        # The node to find large clusters of empty space.
        cluster_mask = pe.Node(MriVolCluster(), name='cluster_mask')
        
        # If the crop_posterior flag is set, then the cluster mask
        # input is the cropped image. Otherwise, the cluster mask
        # input is the time series.
        if crop_posterior:
            # Take the mean image of the time series.
            mean = pe.Node(fsl.MeanImage(), name='mean')
            workflow.connect(input_spec, 'time_series', mean, 'in_file')
            
            # Find the center of gravity from the mean image.
            cog = pe.Node(fsl.ImageStats(), name='cog')
            cog.inputs.op_string = '-C'
            workflow.connect(mean, 'out_file', cog, 'in_file')
            
            # Zero everything posterior to the center of gravity on the
            # mean image.
            crop_back = pe.Node(fsl.ImageMaths(), name='crop_back')
            workflow.connect(mean, 'out_file', crop_back, 'in_file')
            workflow.connect(cog, ('out_stat', _gen_crop_op_string),
                             crop_back, 'op_string')
            workflow.connect(crop_back, 'out_file', cluster_mask, 'in_file')
        else:
            workflow.connect(input_spec, 'time_series', cluster_mask, 'in_file')
        
        # Convert the cluster labels to a binary mask.
        binarize = pe.Node(fsl.BinaryMaths(), name='binarize')
        binarize.inputs.operation = 'min'
        binarize.inputs.operand_value = 1
        workflow.connect(cluster_mask, 'out_cluster_file', binarize, 'in_file')
        
        # Invert the binary mask.
        inv_mask_xfc = fsl.maths.MathsCommand(args='-sub 1 -mul -1',
                                              out_file='mask.nii.gz')
        inv_mask = pe.Node(inv_mask_xfc, name='inv_mask')
        workflow.connect(binarize, 'out_file', inv_mask, 'in_file')
        
        # Upload the mask to XNAT.
        upload_mask_xfc = XNATUpload(project=self.project, resource=MASK_RSC,
                                     modality='MR')
        upload_mask = pe.Node(upload_mask_xfc, name='upload_mask')
        workflow.connect(input_spec, 'subject', upload_mask, 'subject')
        workflow.connect(input_spec, 'session', upload_mask, 'session')
        workflow.connect(input_spec, 'scan', upload_mask, 'scan')
        workflow.connect(inv_mask, 'out_file', upload_mask, 'in_files')
        
        # The output is the mask file.
        output_spec = pe.Node(IdentityInterface(fields=['mask']),
                                                name='output_spec')
        workflow.connect(inv_mask, 'out_file', output_spec, 'mask')
        
        self._configure_nodes(workflow)
        
        self.logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)
        
        return workflow


def _gen_crop_op_string(cog):
    """
    :param cog: the center of gravity
    :return: the crop -roi option
    """
    return "-roi 0 -1 %d -1 0 -1 0 -1" % cog[1]
