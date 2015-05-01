import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces import fsl
from ..interfaces import (XNATUpload, MriVolCluster)
from .workflow_base import WorkflowBase
from qiutil.logging import logger

RESOURCE = 'mask'
"""The XNAT mask resource name."""

TIME_SERIES = 'scan_ts'
"""The XNAT scan time series resource name."""


def run(project, subject, session, scan, time_series, **opts):
    """
    Creates a :class:`qipipe.pipeline.mask.MaskWorkflow` and runs it
    on the given inputs.
    
    :param project: the project name
    :param subject: the input subject
    :param session: the input session
    :param scan: the input scan number
    :param time_series: the input 4D NiFTI time series to mask
    :param opts: additional :class:`MaskWorkflow` initialization parameters
    :return: the XNAT mask resource name
    """
    wf_gen = MaskWorkflow(project, **opts)
    
    return wf_gen.run(subject, session, scan, time_series)


class MaskWorkflow(WorkflowBase):
    """
    The MaskWorkflow class builds and executes the mask workflow.
    
    The workflow creates a mask to subtract extraneous tissue for a given
    input session 4D NiFTI time series. The new mask is uploaded to XNAT
    as a session resource named ``mask``.
    
    The mask workflow input is the `input_spec` node consisting of
    the following input fields:
     
     - subject: the XNAT subject name
     
     - session: the XNAT session name
     
     - scan: the XNAT scan number
     
     - time_series: the 4D NiFTI series image file
    
    The mask workflow output is the `output_spec` node consisting of the
    following output field:
    
    - `mask`: the mask file
    
    The optional workflow configuration file can contain the following
    sections:
    
    - ``fsl.MriVolCluster``: the
        :class:`qipipe.interfaces.mri_volcluster.MriVolCluster`
        interface options
    """
    
    def __init__(self, project, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.
        
        :param project: the XNAT project name
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer options, as well as the following options:
        :keyword base_dir: the workflow execution directory
            (default is a new temp directory)
        """
        super(MaskWorkflow, self).__init__(project, logger(__name__), **opts)
        
        base_dir = opts.get('base_dir')
        self.workflow = self._create_workflow(base_dir)
        """The mask creation workflow."""
    
    def run(self, subject, session, scan, time_series):
        """
        Runs the mask workflow on the scan NiFTI files for the given
        time series.
        
        :param subject: the input subject
        :param session: the input session
        :param scan: the input scan number
        :param time_series: the input 3D NiFTI time series to mask
        :return: the mask XNAT resource name
        """
        self._logger.debug("Creating the mask for the %s %s scan %d time series"
                           " %s..." % (subject, session, scan, time_series))
        self.set_inputs(subject, session, scan, time_series)
        # Execute the workflow.
        self._run_workflow(self.workflow)
        self._logger.debug("Created the %s %s scan %s time series %s mask XNAT"
                           " resource %s." %
                           (subject, session, scan, time_series, RESOURCE))
        
        # Return the mask XNAT resource name.
        return RESOURCE
    
    def set_inputs(self, subject, session, scan, time_series):
        # Set the inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.time_series = time_series
    
    def _create_workflow(self, base_dir=None):
        """
        Creates the mask workflow.
        
        :param base_dir: the workflow execution directory
        :return: the Workflow object
        """
        self._logger.debug('Creating the mask reusable workflow...')
        
        if not base_dir:
            base_dir = tempfile.mkdtemp()
        workflow = pe.Workflow(name='mask', base_dir=base_dir)
        
        # The workflow input.
        in_fields = ['subject', 'session', 'scan', 'time_series']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        
        # Get a mean image from the DCE data.
        dce_mean = pe.Node(fsl.MeanImage(), name='dce_mean')
        workflow.connect(input_spec, 'time_series', dce_mean, 'in_file')
        
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
        # Find large clusters of empty space on the cropped image.
        cluster_mask = pe.Node(MriVolCluster(), name='cluster_mask')
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
        mask_name = pe.Node(mask_name_func, run_without_submitting=True,
                            name='mask_name')
        workflow.connect(input_spec, 'subject', mask_name, 'subject')
        workflow.connect(input_spec, 'session', mask_name, 'session')
        
        # Invert the binary mask.
        inv_mask = pe.Node(fsl.maths.MathsCommand(args='-sub 1 -mul -1'),
                           name='inv_mask')
        workflow.connect(binarize, 'out_file', inv_mask, 'in_file')
        workflow.connect(mask_name, 'out_file', inv_mask, 'out_file')
        
        # Upload the mask to XNAT.
        upload_mask_xfc = XNATUpload(project=self.project, resource=RESOURCE,
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
        
        self._logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)
        
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
