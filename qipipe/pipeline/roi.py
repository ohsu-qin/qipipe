import os
import re
import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
import qiutil
from qiutil.logging import logger
from ..interfaces import (ConvertBoleroMask, XNATUpload)
from .workflow_base import WorkflowBase
from .pipeline_error import PipelineError

ROI_RESOURCE = 'roi'
"""The XNAT ROI resource name."""


def run(project, subject, session, scan, time_series, *inputs, **opts):
    """
    Runs the ROI workflow on the given session BOLERO mask files.

    :param project: the project name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param time_series: the 4D scan time series
    :param inputs: the :meth:`ROIWorkflow.run`
        (lesion number, slice sequence number, in_file) inputs
    :param opts: the :class:`ROIWorkflow` initializer options
    :return: the :meth:`ROIWorkflow.run` result
    """
    # Make the workflow.
    roi_wf = ROIWorkflow(project, **opts)
    # Run the workflow.
    return roi_wf.run(subject, session, scan, time_series, *inputs)


class ROIWorkflow(WorkflowBase):
    """
    The ROIWorkflow class builds and executes the ROI workflow which
    converts the BOLERO mask ``.bqf`` files to NiFTI.

    The ROI workflow input is the *input_spec* node consisting of the
    following input fields:

    - *subject*: the subject name

    - *session*: the session name

    - *scan*: the scan number

    - *time_series*: the 4D time series file path

    - *lesion*: the lesion number

    - *slice_sequence_number*: the one-based slice sequence number

    - *in_file*: the BOLERO mask file to convert

    The output NiFTI file is named
    lesion*lesion*\ ``_slice``\ *slice*\ ``.nii.gz``, where:
    
    - *lesion* is the lesion number
    
    - *slice* is the one-based slice index, zero-padded to length
      two if necessary
    
    Example output file names are ``lesion1_slice09.nii.gz`` or
    ``lesion2_slice12.nii.gz``.
    """

    def __init__(self, project, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param project: the XNAT project name
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer options
        """
        super(ROIWorkflow, self).__init__(project, logger(__name__), **opts)
        self.workflow = self._create_workflow(**opts)
        """The ROI workflow."""

    def run(self, subject, session, scan, time_series, *inputs):
        """
        Runs the ROI workflow on the given session scan images.

        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param time_series: the 4D scan time series file path
        :param inputs: the input
            (lesion number, slice sequence number, in_file)
            tuples to convert
        :return: the XNAT converted ROI resource name, or None if
            there were no inputs
        """
        if not inputs:
            self._logger.debug("Skipping the %s workflow on %s %s scan %d,"
                               "since there are no inputs to convert." %
                               (self.workflow.name, subject, session, scan))
            return None
        # Set the inputs.
        self._set_inputs(subject, session, scan, time_series, *inputs)
        # Execute the workflow.
        self._logger.debug("Executing the %s workflow on %s %s scan %d..." %
                         (self.workflow.name, subject, session, scan))
        self._run_workflow(self.workflow)
        self._logger.debug("Executed the %s workflow on %s %s scan %d." %
                         (self.workflow.name, subject, session, scan))

        # Return the resource name.
        return ROI_RESOURCE

    def _set_inputs(self, subject, session, scan, time_series, *inputs):
        """
        Sets the workflow inputs.

        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param time_series: the 4D scan time series
        :param inputs: the input (lesion, slice, in_file) tuples to
            convert
        """
        # Set the execution workflow inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.time_series = time_series

        # Unpack and roll up the ROI inputs into separate iterable
        # lists.
        lesions = [roi.lesion for roi in inputs]
        slice_seq_nbrs = [roi.slice for roi in inputs]
        in_files = [roi.location for roi in inputs]
        iter_dict = dict(lesion=lesions, slice_sequence_number=slice_seq_nbrs,
                         in_file=in_files)
        iterables = iter_dict.items()
        iter_roi = self.workflow.get_node('iter_roi')
        iter_roi.iterables = iterables
        # Iterate over the ROI input fields in lock-step.
        iter_roi.synchronize = True

    def _create_workflow(self, **opts):
        """
        Makes the ROI execution workflow.

        The execution workflow input is the *input_spec* node consisting of
        the following input fields:

        - *subject*: the subject name

        - *session*: the session name

        - *scan*: the scan number

        - *time_series*: the 4D scan time series
        
        In addition, the workflow runner has the responsibility of setting the
        ``iter_roi`` synchronized (lesion, slice_sequence_number, in_file)
        iterables.

        :param opts: the workflow creation options:
        :return: the execution workflow
        """
        self._logger.debug("Creating the ROI execution workflow...")

        # The execution workflow.
        exec_wf = pe.Workflow(name='roi_exec', base_dir=self.base_dir)

        # The ROI workflow input.
        input_fields = ['subject', 'session', 'scan', 'time_series', 'resource']
        input_spec = pe.Node(IdentityInterface(fields=input_fields),
                             name='input_spec')
        input_spec.inputs.resource = ROI_RESOURCE

        # The input ROI tuples are iterable.
        iter_roi_fields = ['lesion', 'slice_sequence_number', 'in_file']
        iter_roi = pe.Node(IdentityInterface(fields=iter_roi_fields),
                           name='iter_roi')

        # The output file base name.
        basename_xfc = Function(input_names=['lesion', 'slice_sequence_number'],
                                output_names=['basename'],
                                function=base_name)
        basename = pe.Node(basename_xfc, run_without_submitting=True,
                           name='basename')
        exec_wf.connect(iter_roi, 'lesion', basename, 'lesion')
        exec_wf.connect(iter_roi, 'slice_sequence_number',
                        basename, 'slice_sequence_number')
        
        # Convert the input file.
        convert = pe.Node(ConvertBoleroMask(), name='convert')
        exec_wf.connect(iter_roi, 'in_file', convert, 'in_file')
        exec_wf.connect(iter_roi, 'slice_sequence_number',
                        convert, 'slice_sequence_number')
        exec_wf.connect(input_spec, 'time_series', convert, 'time_series')
        exec_wf.connect(basename, 'basename', convert, 'out_base')

        # Upload the ROI results into the XNAT ROI resource.
        upload_roi_xfc = XNATUpload(project=self.project,
                                    resource=ROI_RESOURCE, modality='MR')
        upload_roi = pe.JoinNode(upload_roi_xfc, joinsource='iter_roi',
                                 joinfield='in_files', name='upload_roi')
        exec_wf.connect(input_spec, 'subject', upload_roi, 'subject')
        exec_wf.connect(input_spec, 'session', upload_roi, 'session')
        exec_wf.connect(input_spec, 'scan', upload_roi, 'scan')
        exec_wf.connect(convert, 'out_file', upload_roi, 'in_files')

        self._logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self.depict_workflow(exec_wf)

        return exec_wf


### Utility functions called by the workflow nodes. ###

def base_name(lesion, slice_sequence_number):
    """
    :param lesion: the lesion number
    :param slice_sequence_number: the
        :class:`qipipe.interfaces.ConvertBoleroMask` *slice_sequence_number*
    :return: the base name to use
    """
    return "lesion%d_slice%02d" % (lesion, slice_sequence_number)
