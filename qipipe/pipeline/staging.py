import os
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces.dcmstack import DcmStack
from ..interfaces import (Gate, FixDicom, Compress, XNATFind, XNATUpload)
import qixnat
from .workflow_base import WorkflowBase
from qiutil.logging import logger
from ..staging import iterator


def set_workflow_inputs(exec_wf, scan_input, dest=None):
    """
    Sets the given execution workflow inputs.
    The execution workflow must have the same input and iterable
    node names and fields as the :class:`StagingWorkflow` workflow.

    :param exec_wf: the workflow to execute
    :param scan_input: the :class:`qipipe.staging.iterator.ScanInput`
        object
    :param dest: the TCIA staging destination directory (default is
        the current working directory)
    """
    # Set the top-level inputs.
    input_spec = exec_wf.get_node('input_spec')
    input_spec.inputs.collection = scan_input.collection
    input_spec.inputs.subject = scan_input.subject
    input_spec.inputs.session = scan_input.session
    input_spec.inputs.scan = scan_input.scan

    # Set the iterables.
    set_workflow_iterables(exec_wf, scan_input, dest)


def set_workflow_iterables(exec_wf, scan_input, dest=None):
    """
    Sets the given execution workflow iterables.
    The execution workflow must have the same iterable
    node names and fields as the :class:`StagingWorkflow` workflow.

    :param exec_wf: the workflow to execute
    :param scan_input: the :class:`qipipe.staging.iterator.ScanInput`
        object
    :param dest: the TCIA staging destination directory (default is
        the current working directory)
    """
    # The input volumes to stage.
    volumes = scan_input.iterators.dicom.keys()
    # Make a staging area subdirectory for each volumes.
    stg_dict = _create_staging_area(scan_input.subject, scan_input.session,
                                    volumes, dest)
    # The staging destination directories are pair-wise synchronized
    # with the input volumes.
    dests = [stg_dict[volume] for volume in volumes]
    iterables = dict(volume=volumes, dest=dests).items()

    # Set the volume iterator inputs.
    iter_volume = exec_wf.get_node('iter_volume')
    iter_volume.iterables = iterables
    # Iterate over the volume and dest input fields in lock-step.
    iter_volume.synchronize = True

    # Set the DICOM file iterator inputs.
    iter_dicom = exec_wf.get_node('iter_dicom')
    iter_dicom.itersource = ('iter_volume', 'volume')
    iter_dicom.iterables = ('dicom_file', scan_input.iterators.dicom)


def _create_staging_area(subject, session, volumes, dest=None):
    """
    :param subject: the subject name
    :param session: the session name
    :param volumes: the input volumes
    :param dest: the TCIA staging destination directory (default is
        the current working directory)
    :return: the {volume number: directory} dictionary
    """
    # The staging location.
    dest = os.path.abspath(dest) if dest else os.getcwd()
    # Collect the {volume: destination} dictionary.
    return {volume: _make_volume_staging_directory(dest, subject, session, volume)
            for volume in volumes}


def _make_volume_staging_directory(dest, subject, session, volume):
    """
    Returns the dest/subject/session/volume directory path in which to
    place DICOM files for TCIA upload. Creates the directory, if
    necessary.

    :return: the target volume directory path
    """
    path = os.path.join(dest, subject, session, str(volume))
    if not os.path.exists(path):
        os.makedirs(path)

    return path


class StagingWorkflow(WorkflowBase):
    """
    The StagingWorkflow class builds and executes the staging Nipype workflow.
    The staging workflow includes the following steps:

    - Group the input DICOM images into volume.

    - Fix each input DICOM file header using the
      :class:`qipipe.interfaces.fix_dicom.FixDicom` interface.

    - Compress each corrected DICOM file.

    - Upload each compressed DICOM file into XNAT.

    - Stack each new volume's 2-D DICOM files into a 3-D volume NiFTI file
      using the DcmStack_ interface.

    - Upload each new volume stack into XNAT.

    - Make the CTP_ QIN-to-TCIA subject id map.

    - Collect the id map and the compressed DICOM images into a target
      directory in collection/subject/session/volume format for TCIA
      upload.

    The staging workflow input is the *input_spec* node consisting of
    the following input fields:

    - *collection*: the collection name

    - *subject*: the subject name

    - *session*: the session name

    - *scan*: the scan number

    The staging workflow has two iterables:

    - the *iter_volume* node with input fields *volume* and *dest*

    - the *iter_dicom* node with input fields *volume* and *dicom_file*

    These iterables must be set prior to workflow execution. The
    *iter_volume* *dest* input is the destination directory for
    the *iter_volume* *volume*.

    The *iter_dicom* node *itersource* is the ``iter_volume.volume``
    field. The ``iter_dicom.dicom_file`` iterables is set to the
    {volume: [DICOM files]} dictionary.

    The DICOM files to upload to TCIA are placed in the destination
    directory in the following hierarchy:

        ``/path/to/dest/``
          *subject*\ /
            *session*\ /
              ``volume``\ *volume number*\ /
                *file*
                ...

    where:

    * *subject* is the subject name, e.g. ``Breast011``

    * *session* is the session name, e.g. ``Session03``

    * *volume number* is determined by the
      :attr:`qipipe.staging.collection.Collection.patterns`
      :attr:`qipipe.staging.collection.Patterns.volume` DICOM tag

    * *file* is the DICOM file name

    The staging workflow output is the *output_spec* node consisting
    of the following output field:

    - *image*: the 3D volume stack NiFTI image file

    .. _CTP: https://wiki.cancerimagingarchive.net/display/Public/Image+Submitter+Site+User%27s+Guide
    .. _DcmStack: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.dcmstack.html
    """

    def __init__(self, project, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param project: the XNAT project name
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer options, as well as the following options:
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        """
        super(StagingWorkflow, self).__init__(project, logger(__name__), **opts)

        # Make the workflow.
        base_dir = opts.get('base_dir')
        self.workflow = self._create_workflow(base_dir=base_dir)
        """
        The staging workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """

    def set_inputs(self, scan_input, dest=None):
        """
        Sets the staging workflow inputs.

        :param scan_input: the :class:`qipipe.staging.iterator.ScanInput`
            object
        :param dest: the TCIA staging destination directory (default is
            the current working directory)
        """
        set_workflow_inputs(self.workflow, scan_input, dest)

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
        self._logger.debug('Creating the DICOM processing workflow...')

        # The Nipype workflow object.
        workflow = pe.Workflow(name='staging', base_dir=base_dir)

        # The workflow input.
        in_fields = ['collection', 'subject', 'session', 'scan']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self._logger.debug("The %s workflow input node is %s with fields %s" %
                         (workflow.name, input_spec.name, in_fields))

        # Create the scan, if necessary. The gate blocks upload until the
        # scan is created.
        find_scan_xfc = XNATFind(project=self.project, modality='MR', create=True)
        find_scan = pe.Node(find_scan_xfc, name='find_scan')
        workflow.connect(input_spec, 'subject', find_scan, 'subject')
        workflow.connect(input_spec, 'session', find_scan, 'session')
        workflow.connect(input_spec, 'scan', find_scan, 'scan')
        scan_gate_xfc = Gate(fields=['scan', 'xnat_id'])
        scan_gate = pe.Node(scan_gate_xfc, run_without_submitting=True,
                            name='scan_gate')
        workflow.connect(input_spec, 'scan', scan_gate, 'scan')
        workflow.connect(find_scan, 'xnat_id', scan_gate, 'xnat_id')

        # The volume iterator.
        iter_volume_fields = ['volume', 'dest']
        iter_volume = pe.Node(IdentityInterface(fields=iter_volume_fields),
                              name='iter_volume')
        self._logger.debug("The %s workflow volume iterable node is %s with"
                           " fields %s" % (workflow.name, iter_volume.name,
                                           iter_volume_fields))

        # The DICOM file iterator.
        iter_dicom_fields = ['volume', 'dicom_file']
        iter_dicom = pe.Node(IdentityInterface(fields=iter_dicom_fields),
                             name='iter_dicom')
        self._logger.debug("The %s workflow DICOM iterable node is %s with"
                           " iterable source %s and iterables"
                           " ('%s', {%s: [%s]})" %
                           (workflow.name, iter_dicom.name, iter_volume.name,
                           'dicom_file', 'volume', 'DICOM files'))
        workflow.connect(iter_volume, 'volume', iter_dicom, 'volume')

        # Fix the AIRC DICOM tags.
        fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
        workflow.connect(input_spec, 'collection', fix_dicom, 'collection')
        workflow.connect(input_spec, 'subject', fix_dicom, 'subject')
        workflow.connect(iter_dicom, 'dicom_file', fix_dicom, 'in_file')

        # Compress the corrected DICOM files.
        compress_dicom = pe.Node(Compress(), name='compress_dicom')
        workflow.connect(fix_dicom, 'out_file', compress_dicom, 'in_file')
        workflow.connect(iter_volume, 'dest', compress_dicom, 'dest')

        # Collect the compressed DICOM files, as follows:
        # * The volume DICOM files are collected into a list.
        # * The volume DICOM file lists are merged into a scan
        #   DICOM file list.
        # * The combined scan DICOM file list is uploaded to XNAT.
        #
        # The collection involves a two-step JoinNode, first on
        # the volume, then on the scan. The second scan JoinNode
        # calls a merge function to merge the lists from all of
        # the first JoinNodes. All of this is necessary for the
        # following reasons:
        # * XNAT concurrent file upload tasks to the same resource
        #   results in a pyxnat.core.errors.DatabaseError with the
        #   useless message:
        #   'The server encountered an unexpected condition'
        # * Nipype Merge is inflexible (see the merge function
        #   comment below)
        # Since concurrent upload is not supported, all of the
        # compressed DICOM files must be collected into a single
        # list which is uploaded in a single upload task when
        # deployed in a clustered environment such as SGE.
        collect_vol_dicom_xfc = IdentityInterface(fields=['dicom_files'])
        collect_vol_dicom = pe.JoinNode(collect_vol_dicom_xfc,
                                        joinsource='iter_dicom',
                                        joinfield='dicom_files',
                                        name='collect_vol_dicom')
        workflow.connect(compress_dicom, 'out_file',
                         collect_vol_dicom, 'dicom_files')
        collect_scan_dicom_xfc = Function(input_names=['lists'],
                                          output_names=['out_list'],
                                          function=merge)
        collect_scan_dicom = pe.JoinNode(collect_scan_dicom_xfc,
                                         run_without_submitting=True,
                                         joinsource='iter_volume',
                                         joinfield='lists',
                                         name='collect_scan_dicom')
        workflow.connect(collect_vol_dicom, 'dicom_files',
                         collect_scan_dicom, 'lists')

        # Upload the compressed DICOM files.
        upload_dicom_xfc = XNATUpload(project=self.project, resource='DICOM',
                                      skip_existing=True)
        upload_dicom = pe.Node(upload_dicom_xfc, name='upload_dicom')
        workflow.connect(input_spec, 'subject', upload_dicom, 'subject')
        workflow.connect(input_spec, 'session', upload_dicom, 'session')
        workflow.connect(scan_gate, 'scan', upload_dicom, 'scan')
        workflow.connect(collect_scan_dicom, 'out_list',
                         upload_dicom, 'in_files')

        # The volume file name format.
        vol_fmt_xfc = Function(input_names=['collection'],
                               output_names=['format'],
                               function=volume_format)
        vol_fmt = pe.Node(vol_fmt_xfc, run_without_submitting=True,
                          name='volume_format')
        workflow.connect(input_spec, 'collection', vol_fmt, 'collection')

        # Stack the scan volume into a 3D NiFTI file.
        stack_xfc = DcmStack(embed_meta=True)
        stack = pe.JoinNode(stack_xfc, joinsource='iter_dicom',
                            joinfield='dicom_files', name='stack')
        workflow.connect(fix_dicom, 'out_file', stack, 'dicom_files')
        workflow.connect(vol_fmt, 'format', stack, 'out_format')

        # Force the T1 3D upload to follow DICOM upload.
        # Note: XNAT fails app. 80% into the scan upload. It appears to be
        # a concurrency conflict, possibly arising from one of the following
        # causes:
        # * the pyxnat config in $HOME/.xnat/xnat.cfg specifies a temp
        #   directory that *is not* shared by all concurrent jobs,
        #   resulting in inconsistent cache content
        # * the pyxnat config in $HOME/.xnat/xnat.cfg specifies a temp
        #   directory that *is* shared by some concurrent jobs,
        #   resulting in unsynchronized pyxnat write conflicts across
        #   jobs
        # * the non-reentrant pyxnat's custom non-http2lib cache is
        #   corrupted
        # * an XNAT archive directory access race condition
        #
        # The cause cannot be isolated for the following reasons:
        # * there is no useful Nipype error or log message
        # * the error is sporadic and unreproducible
        # * Nipype swallows non-nipype Python log messages, causing the
        #   upload and pyxnat log messages to disappear
        #
        # This gate task serializes upload to prevent potential XNAT access
        # conflicts.
        upload_3d_gate_xfc = Gate(fields=['out_file', 'xnat_files'])
        upload_3d_gate = pe.Node(upload_3d_gate_xfc, name='upload_3d_gate')
        workflow.connect(upload_dicom, 'xnat_files', upload_3d_gate, 'xnat_files')
        workflow.connect(stack, 'out_file', upload_3d_gate, 'out_file')

        # Upload the 3D NiFTI stack files to XNAT.
        upload_3d_xfc = XNATUpload(project=self.project, resource='NIFTI',
                                   skip_existing=True)
        upload_3d = pe.Node(upload_3d_xfc, name='upload_3d')
        workflow.connect(input_spec, 'subject', upload_3d, 'subject')
        workflow.connect(input_spec, 'session', upload_3d, 'session')
        workflow.connect(scan_gate, 'scan', upload_3d, 'scan')
        # 3D upload is gated by DICOM upload.
        workflow.connect(upload_3d_gate, 'out_file', upload_3d, 'in_files')

        # The output is the 3D NiFTI stack file. Make the output a Gate node
        # rather than an IdentityInterface in order to prevent nipype from
        # overzealously pruning it as extraneous.
        output_spec_xfc = Gate(fields=['image', 'xnat_files'])
        output_spec = pe.Node(output_spec_xfc, run_without_submitting=True,
                              name='output_spec')
        workflow.connect(stack, 'out_file', output_spec, 'image')
        workflow.connect(upload_3d, 'xnat_files', output_spec, 'xnat_files')

        # Instrument the nodes for cluster submission, if necessary.
        self._configure_nodes(workflow)

        self._logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)

        return workflow


def volume_format(collection):
    """
    :param collection: the collection name
    :return: the volume file name format
    """
    from qipipe.staging import collections

    coll = collections.with_name(collection)

    return "volume%%(%s)03d" % coll.patterns.volume


def merge(lists):
    """
    Merges the given lists. This function works around the following
    Nipype Merge node limitation:

    * The Nipype Merge initializer requires the number of lists to merge.

    This merge function accepts an arbitrary number of lists.

    :param lists: this lists to merge
    :return: the merged lists
    """
    merger = lambda x,y: x + y

    return reduce(merger, lists)
