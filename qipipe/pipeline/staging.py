import os
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.dcmstack import DcmStack
from .. import project
from ..interfaces import (Gate, FixDicom, Compress, XNATUpload)
from ..staging.staging_error import StagingError
from ..helpers import xnat_helper
from .workflow_base import WorkflowBase
from ..helpers.logging_helper import logger
from ..staging import staging_helper


def set_workflow_inputs(exec_wf, collection, subject, session, ser_dicom_dict,
                        dest=None):
    """
    Sets the given execution workflow inputs.
    The execution workflow must have the same input and iterable
    node names and fields as the :class:`StagingWorkflow` workflow.

    :param exec_wf: the workflow to execute
    :param subject: the subject name
    :param session: the session name
    :param ser_dicom_dict: the input {series: directory} dictionary
    :param dest: the TCIA staging destination directory (default is
        the current working directory)
    """
    # Make the staging area.
    ser_list = ser_dicom_dict.keys()
    ser_dests = _create_staging_area(subject, session,
                                     ser_dicom_dict.iterkeys(), dest)
    # Transpose the [(series, directory), ...] tuples into iterable lists.
    sers, dests = map(list, zip(*ser_dests))
    ser_iterables = dict(series=sers, dest=dests).items()

    # Set the inputs.
    input_spec = exec_wf.get_node('input_spec')
    input_spec.inputs.subject = subject
    input_spec.inputs.session = session
    input_spec.inputs.collection = collection

    iter_series = exec_wf.get_node('iter_series')
    iter_series.iterables = ser_iterables
    # Iterate over the series and dest input fields in lock-step.
    iter_series.synchronize = True

    iter_dicom = exec_wf.get_node('iter_dicom')
    iter_dicom.iterables = ('dicom_file', ser_dicom_dict)


def _create_staging_area(subject, session, series_list, dest=None):
    """
    :param subject: the subject name
    :param session: the session name
    :param ser_list: the input series list
    :param dest: the TCIA staging destination directory (default is
        the current working directory)
    :return: the [*(series, directory)*, ...] list
    """
    # The staging location.
    if dest:
        dest = os.path.abspath(dest)
    else:
        dest = os.getcwd()
    # Collect the (series, destination) tuples.
    ser_dest_tuples = []
    for series in series_list:
        # Make the staging directories. Do this before running the
        # workflow in order to avoid a directory creation race
        # condition for distributed nodes that write to the series
        # staging directory.
        ser_dest = _make_series_staging_directory(dest, subject, session,
                                                  series)
        ser_dest_tuples.append((series, ser_dest))

    return ser_dest_tuples


def _make_series_staging_directory(dest, subject, session, series):
    """
    Returns the dest/subject/session/series directory path in which to
    place DICOM files for TCIA upload. Creates the directory, if
    necessary.

    :return: the target series directory path
    """
    path = os.path.join(dest, subject, session, str(series))
    if not os.path.exists(path):
        os.makedirs(path)

    return path


class StagingWorkflow(WorkflowBase):

    """
    The StagingWorkflow class builds and executes the staging Nipype workflow.
    The staging workflow includes the following steps:

    - Group the input DICOM images into series.

    - Fix each input DICOM file header using the
      :class:`qipipe.interfaces.fix_dicom.FixDicom` interface.

    - Compress each corrected DICOM file.

    - Upload each compressed DICOM file into XNAT.

    - Stack each new series's 2-D DICOM files into a 3-D series NiFTI file
      using the DcmStack_ interface.

    - Upload each new series stack into XNAT.

    - Make the CTP_ QIN-to-TCIA subject id map.

    - Collect the id map and the compressed DICOM images into a target
      directory in collection/subject/session/series format for TCIA
      upload.

    The staging workflow input is the *input_spec* node consisting of
    the following input fields:

    - *collection*: the collection name

    - *subject*: the subject name

    - *session*: the session name

    The staging workflow has two iterables:

    - the *iter_series* node with input fields *series and *dest*

    - the *iter_dicom* node with input fields *series* and *dicom_file*

    These iterables must be set prior to workflow execution. The
    *iter_series* *dest* input is the destination directory for
    the *iter_series* *series*.

    The *iter_dicom* node *itersource* is the ``iter_series.series``
    field. The ``iter_dicom.dicom_file`` iterables is set to the
    {series: [DICOM files]} dictionary.

    The DICOM files to upload to TCIA are placed in the destination
    directory in the following hierarchy:

        ``/path/to/dest/``
          *subject*/
            *session*/
              ``Series``*series_number*/
                *file*``.dcm.gz``
                ...

    where:

    * *subject* is the subject name, e.g. ``Breast011``

    * *session* is the session name, e.g. ``Session03``

    * *series_number* is the DICOM Series Number

    * *file* is the DICOM file base name

    The staging workflow output is the *output_spec* node consisting
    of the following output field:

    - *image*: the session series stack NiFTI image file

    .. _CTP: https://wiki.cancerimagingarchive.net/display/Public/Image+Submitter+Site+User%27s+Guide
    .. _DcmStack: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.dcmstack.html
    """

    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :parameter opts: the following keword options:
        :keyword project: the XNAT project (default ``QIN``)
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        """
        cfg_file = opts.get('cfg_file')
        super(StagingWorkflow, self).__init__(logger(__name__), cfg_file)

        # Set the XNAT project.
        if 'project' in opts:
            project(opts['project'])
            self._logger.debug("Set the XNAT project to %s." % project())

        # Make the workflow.
        base_dir = opts.get('base_dir')
        self.workflow = self._create_workflow(base_dir=base_dir)
        """
        The staging workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """

    def set_inputs(self, collection, subject, session, ser_dicom_dict,
                   dest):
        """
        Sets the staging workflow inputs.

        :param subject: the subject name
        :param session: the session name
        :param ser_dicom_dict: the input {series: directory} dictionary
        :param dest: the TCIA staging destination directory
        """
        set_workflow_inputs(self.workflow, collection, subject, session,
                            ser_dicom_dict, dest)

    def run(self):
        """Executes the staging workflow."""
        self._run_workflow(self.workflow)

    def _create_workflow(self, base_dir=None):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.

        :param base_dir: the workflow execution directory
            (default is a new temp directory)
        :return: the new workflow
        """
        self._logger.debug("Creating the DICOM processing workflow...")

        # The Nipype workflow object.
        workflow = pe.Workflow(name='staging', base_dir=base_dir)

        # The workflow input.
        in_fields = ['collection', 'subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self._logger.debug("The %s workflow input node is %s with fields %s" %
                         (workflow.name, input_spec.name, in_fields))

        iter_series_fields = ['series', 'dest']
        iter_series = pe.Node(IdentityInterface(fields=iter_series_fields),
                              name='iter_series')
        self._logger.debug("The %s workflow series iterable node is %s with"
                           " fields %s" % (workflow.name, iter_series.name,
                                           iter_series_fields))

        # The DICOM file iterator.
        iter_dicom_fields = ['series', 'dicom_file']
        iter_dicom = pe.Node(IdentityInterface(fields=iter_dicom_fields),
                             itersource=('iter_series', 'series'),
                             name='iter_dicom')
        self._logger.debug("The %s workflow DICOM iterable node is %s with"
                           " iterable source %s and iterables"
                           " ('%s', {%s: [%s]})" %
                           (workflow.name, iter_dicom.name, iter_series.name,
                           'dicom_file', 'series', 'DICOM files'))
        workflow.connect(iter_series, 'series', iter_dicom, 'series')

        # Fix the AIRC DICOM tags.
        fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
        workflow.connect(input_spec, 'collection', fix_dicom, 'collection')
        workflow.connect(input_spec, 'subject', fix_dicom, 'subject')
        workflow.connect(iter_dicom, 'dicom_file', fix_dicom, 'in_file')

        # Compress the corrected DICOM file.
        compress_dicom = pe.Node(Compress(), name='compress_dicom')
        workflow.connect(fix_dicom, 'out_file', compress_dicom, 'in_file')
        workflow.connect(iter_series, 'dest', compress_dicom, 'dest')

        # Upload the compressed DICOM files to XNAT.
        # Since only one upload task can run at a time, this upload node is
        # a JoinNode that collects the iterated series DICOM files.
        upload_dicom = pe.JoinNode(
            XNATUpload(project=project(), resource='DICOM'),
            joinsource='iter_dicom', joinfield='in_files', name='upload_dicom')
        workflow.connect(input_spec, 'subject', upload_dicom, 'subject')
        workflow.connect(input_spec, 'session', upload_dicom, 'session')
        workflow.connect(iter_series, 'series', upload_dicom, 'scan')
        workflow.connect(compress_dicom, 'out_file', upload_dicom, 'in_files')

        # Stack the scan.
        stack_xfc = DcmStack(
            embed_meta=True, out_format="series%(SeriesNumber)03d")
        stack = pe.JoinNode(
            stack_xfc, joinsource='iter_dicom', joinfield='dicom_files',
            name='stack')
        workflow.connect(fix_dicom, 'out_file', stack, 'dicom_files')

        # Upload the stack to XNAT.
        upload_stack = pe.Node(XNATUpload(project=project()),
                               name='upload_stack')
        workflow.connect(input_spec, 'subject', upload_stack, 'subject')
        workflow.connect(input_spec, 'session', upload_stack, 'session')
        workflow.connect(iter_series, 'series', upload_stack, 'scan')
        workflow.connect(stack, 'out_file', upload_stack, 'in_files')

        # The output is the stack file.
        output_spec = pe.Node(Gate(fields=['image']),
                              name='output_spec')
        workflow.connect(stack, 'out_file', output_spec, 'image')

        self._configure_nodes(workflow)

        self._logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)

        return workflow
