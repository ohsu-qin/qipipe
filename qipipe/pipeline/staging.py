import os
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.dcmstack import DcmStack
from .. import project
from ..interfaces import (Gate, FixDicom, Compress, MapCTP, XNATUpload)
from ..staging.staging_error import StagingError
from ..helpers import xnat_helper
from .workflow_base import WorkflowBase
from ..helpers.logging_helper import logger
from ..staging import staging_helper


def iter_stage(collection, *inputs, **opts):
    """
    Runs the staging workflow on the new AIRC visits in the given
    input directories.

    The new DICOM files to upload to TCIA are placed in the destination
    `dicom` subdirectory in the following hierarchy:

        */path/to/dest/*``dicom/``
            *subject*/
                *session*/
                    series*series_number*/
                        *file*.dcm.gz
                        ...

    where:
    
    * *subject* is the XNAT subject name
    
    * *session* is the XNAT session name
    
    * *series_number* is the DICOM Series Number
    
    * *file* is the DICOM file name

    The new series stack NiFTI files are placed in the destination
    `stacks` subdirectory in the following hierarchy:

        */path/to/dest*``/staged/``
            *subject*/
                *session*/
                    series*series_number*.dcm.gz
                    ...

    :param collection: the AIRC image collection name
    :param inputs: the AIRC source subject directories to stage
    :param opts: the following workflow execution options:
    :keyword dest: the TCIA staging destination directory (default is a
        subdirectory named ``staged`` in the current working directory)
    """
    # Validate that there is a collection
    if not collection:
        raise ValueError('Staging is missing the AIRC collection name')
    
    # Group the new DICOM files into a
    # {subject: {session: [(series, dicom_files), ...]}} dictionary.
    stg_dict = self._detect_visits(collection, *inputs)
    if not stg_dict:
        return

    # The staging location.
    dest_opt = opts.pop('dest', None)
    if dest_opt:
        dest = os.path.abspath(dest_opt)
    else:
        dest = os.path.join(os.getcwd(), 'staged')

    # The workflow subjects.
    subjects = stg_dict.keys()

    # Print a debug message.
    series_cnt = sum(map(len, stg_dict.itervalues()))
    logger(__name__).debug(
        "Staging %d new %s series from %d subjects in %s..." %
        (series_cnt, collection, len(subjects), dest))
    
    # Run the workflow for each session.
    for sbj, sess_dict in stg_dict.iteritems():
        self._logger.debug("Staging subject %s..." % sbj)
        for sess, ser_dicom_dict in sess_dict.iteritems():
            logger(__name__).debug("Staging %s session %s..." % (sbj, sess))
            # Delegate to the workflow executor.
            yield sbj, sess, ser_dicom_dict
            logger(__name__).debug("Staged %s session %s." % (sbj, sess))
        logger(__name__).debug("Staged subject %s." % sbj)
    logger(__name__).debug("Staged %d new %s series from %d subjects in %s." %
                     (series_cnt, collection, len(subjects), dest))
    
    # Make the TCIA subject map.
    staging_helper.create_subject_map(collection, subjects, dest)


def execute_workflow(exec_wf, collection, subject, session, ser_dicom_dict,
                     dest=None):
    """
    Stages the given session's series DICOM files as described in
    :class:`StagingWorkflow`.
    
    The execution workflow must have the same input and iterable
    node names and fields as :class:`StagingWorkflow`.
    
    :param exec_wf: the workflow to execute
    :param ser_dicom_dict: the input [(series, directory), ...] tuples
    """
    # Make the staging area.
    ser_list = ser_dicom_dict.keys()
    ser_dests = _create_staging_area(sbj, sess, ser_list, dest)
    # Transpose the tuples into iterable lists.
    sers, dests = map(list, zip(*ser_dicom_dict))
    ser_iterables = dict(series=sers, dest=dests).items()

    # Set the inputs.
    input_spec = exec_wf.get_node('input_spec')
    input_spec.inputs.subject = subject
    input_spec.inputs.session = session
    input_spec.inputs.collection = collection
    
    iter_series = exec_wf.get_node('iter_series')
    iter_series.iterables = ser_iterables

    iter_dicom = exec_wf.get_node('iter_dicom')
    iter_dicom.iterables = ('dicom_file', ser_dicom_dict)

    # Execute the workflow.
    self._run_workflow(exec_wf)

def _create_staging_area(subject, session, series_list, dest):
    """
    :return: the [(series, directory), ...] list
    """
    # Collect the (series, destination) tuples.
    ser_dest_tuples = []
    for series in series_list:
        # Make the staging directories. Do this before running the
        # workflow in order to avoid a directory creation race
        # condition for distributed nodes that write to the series
        # staging directory.
        ser_dest = self._make_series_staging_directory(dest, subject,
                                                       session, series)
        ser_dest_tuples.append((series, ser_dest))
    
    return ser_dest_tuples


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

    - *subject*: the subject name

    - *session*: the session name

    - *series*: the scan number
    
    The staging workflow has two iterables:
    
    - the *input_spec* *series* and *dest* fields
    
    - the *iter_dicom* *dicom_file* field
    
    These iterables must be set prior to workflow execution. The
    *input_spec* iterables is set to the session scan numbers.
    
    The *iter_dicom* node *itersource* is the ``iter_series.series``
    field. The ``iter_dicom.dicom_file`` iterables is set to the
    {series: [DICOM files]} dictionary.
    Runs the staging workflow on the new AIRC visits in the given
    input directories.

    The new DICOM files to upload to TCIA are placed in the destination
    `dicom` subdirectory in the following hierarchy:

        ``/path/to/dest/dicom/``
            *subject*/
                *session*/
                    ``Series``*series_number*/
                        *file*``.dcm.gz``
                        ...

    where:
    
    * *subject* is the subject name, e.g. ``Breast011``
    
    * *session* is the session name, e.g. ``Session03``
    
    * *series_number* is the DICOM Series Number
    
    * *file* is the DICOM file name
   
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
        self._logger.debug("The %s workflow series iterable node is %s with fields %s" %
                         (workflow.name, iter_series.name, iter_series_fields))
  
        # The DICOM file iterator.
        iter_dicom_fields = ['series', 'dicom_file']
        iter_dicom = pe.Node(IdentityInterface(fields=iter_dicom_fields),
                             name='iter_dicom')
        self._logger.debug("The %s workflow DICOM iterable node is %s with iterable"
                           " source %s and iterables ('%s', {%s: [%s]})" %
                           (workflow.name, iter_dicom.name, iter_series.name,
                           'dicom_file', 'series', 'DICOM files'))

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
        upload_stack = pe.Node(XNATUpload(project=project()), name='upload_stack')
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

    def _make_series_staging_directory(self, dest, subject, session, series):
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


def _join_files(*fnames):
    import os

    return os.path.join(*fnames)
