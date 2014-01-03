import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces import fsl
from .. import project
from ..interfaces import (XNATUpload, MriVolCluster)
from .workflow_base import WorkflowBase
from ..helpers.logging_helper import logger


MASK = 'mask'
"""The XNAT mask reconstruction name."""

TIME_SERIES = 'scan_ts'
"""The XNAT scan time series reconstruction name."""


def run(input_dict, **opts):
    """
    Creates a :class:`qipipe.pipeline.mask.MaskWorkflow` and runs it
    on the given inputs.
    
    :param input_dict: the :meth:`qipipe.pipeline.mask.MaskWorkflow.run`
        inputs
    :param opts: the :meth:`qipipe.pipeline.mask.MaskWorkflow.__init__`
        options
    :return: the XNAT mask reconstruction name
    """
    return MaskWorkflow(**opts).run(input_dict)


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
    
    def __init__(self, cfg_file=None, base_dir=None):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.
        
        :keyword base_dir: the workflow execution directory
            (default is a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        """
        super(MaskWorkflow, self).__init__(logger(__name__), cfg_file)
        
        self.workflow = self._create_workflow(base_dir)
        """The mask creation workflow."""
    
    def run(self, input_dict):
        """
        Runs the mask workflow on the scan NiFTI files for the given
        (subject, session) inputs.
        
        :param input_dict: the input *{subject: {session: time series}}* dictionary
        :return: the mask XNAT reconstruction name
        """
        sbj_cnt = len(input_dict)
        sess_cnt = sum(map(len, input_dict.values()))
        self._logger.debug("Masking %d sessions from %d subjects..." %
            (sess_cnt, sbj_cnt))
        for sbj, sess_dict in input_dict.iteritems():
            self._logger.debug("Masking subject %s..." % sbj)
            for sess, time_series in sess_dict.iteritems():
                self._logger.debug("Masking the %d %s %s time series..." %
                    (len(time_series), sbj, sess))
                self._mask_session(sbj, sess, time_series)
                self._logger.debug("Masked the %s %s time series." % (sbj, sess))
            self._logger.debug("Masked the subject %s time series." % sbj)
        self._logger.debug("Masked %d sessions from %d subjects." %
            (sess_cnt, sbj_cnt))
        
        # Execute the workflow.
        self._run_workflow(self.workflow)
        
        # Return the mask XNAT reconstruction name.
        return MASK
    
    def _mask_session(self, subject, session, time_series):
        # Set the inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.time_series = time_series
        
        # Execute the workflow.
        self._run_workflow(self.workflow)
    
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
        in_fields = ['subject', 'session', 'time_series']
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
        mask_name = pe.Node(mask_name_func, name='mask_name')
        workflow.connect(input_spec, 'subject', mask_name, 'subject')
        workflow.connect(input_spec, 'session', mask_name, 'session')
        
        # Invert the binary mask.
        inv_mask = pe.Node(fsl.maths.MathsCommand(args='-sub 1 -mul -1'),
                           name='inv_mask')
        workflow.connect(binarize, 'out_file', inv_mask, 'in_file')
        workflow.connect(mask_name, 'out_file', inv_mask, 'out_file')
        
        # Upload the mask to XNAT.
        upload_mask = pe.Node(XNATUpload(project=project(), resource=MASK),
                              name='upload_mask')
        workflow.connect(input_spec, 'subject', upload_mask, 'subject')
        workflow.connect(input_spec, 'session', upload_mask, 'session')
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
