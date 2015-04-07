import os
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.dcmstack import DcmStack
from .. import project
from ..interfaces import (Gate, FixDicom, Compress, XNATFind, XNATCopy)
import qixnat
from .workflow_base import WorkflowBase
from qiutil.logging import logger
from ..staging import staging_helper


def set_workflow_inputs(exec_wf, collection, subject, session,
                        vol_dcm_dict, dest=None):
    """
    Sets the given execution workflow inputs.
    The execution workflow must have the same input and iterable
    node names and fields as the :class:`StagingWorkflow` workflow.

    :param exec_wf: the workflow to execute
    :param collection: the AIRC collection name
    :param subject: the subject name
    :param session: the session name
    :param vol_dcm_dict: the *{volume number: [dicom files]}* dictionary
    :param dest: the TCIA staging destination directory (default is
        the current working directory)
    """
    # The input volumes to stage.
    volumes = vol_dcm_dict.keys()
    # Make a staging area subdirectory for each volumes.
    stg_dict = _create_staging_area(subject, session, volumes, dest)
    # The staging destination directories are pair-wise synchronized
    # with the input volumes.
    dests = [stg_dict[volume] for volume in volumes]
    iterables = dict(volume=volumes, dest=dests).items()

    # Set the top-level inputs.
    input_spec = exec_wf.get_node('input_spec')
    input_spec.inputs.collection = collection
    input_spec.inputs.subject = subject
    input_spec.inputs.session = session

    # Set the volume iterator inputs.
    iter_volume = exec_wf.get_node('iter_volume')
    iter_volume.iterables = iterables
    # Iterate over the volume and dest input fields in lock-step.
    iter_volume.synchronize = True

    # Set the DICOM file iterator inputs.
    iter_dicom = exec_wf.get_node('iter_dicom')
    iter_dicom.itersource = ('iter_volume', 'volume')
    iter_dicom.iterables = ('dicom_file', vol_dcm_dict)


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
      :class:`qipipe.staging.airc_collection.AIRCCollection` volume
      DICOM tag

    * *file* is the DICOM file name

    The staging workflow output is the *output_spec* node consisting
    of the following output field:

    - *image*: the 3D volume stack NiFTI image file

    .. _CTP: https://wiki.cancerimagingarchive.net/display/Public/Image+Submitter+Site+User%27s+Guide
    .. _DcmStack: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.dcmstack.html
    """

    def __init__(self, scan, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param scan: the scan number
        :parameter opts: the following keword options:
        :keyword project: the XNAT project (default ``QIN``)
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        """
        cfg_file = opts.pop('cfg_file', None)
        super(StagingWorkflow, self).__init__(logger(__name__), cfg_file)

        # Set the XNAT project.
        prj = opts.pop('project', None)
        if prj:
            project(prj)
            self._logger.debug("Set the XNAT project to %s." % prj)

        # Make the workflow.
        self.workflow = self._create_workflow(scan, **opts)
        """
        The staging workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """

    def set_inputs(self, collection, subject, session, vol_dcm_dict,
                   dest=None):
        """
        Sets the staging workflow inputs.

        :param collection: the collection name
        :param subject: the subject name
        :param session: the session name
        :param vol_dcm_dict: the *{volume: [dicom files]}* dictionary
        :param dest: the TCIA staging destination directory (default is
            the current working directory)
        """
        set_workflow_inputs(self.workflow, collection, subject, session,
                            vol_dcm_dict, dest)

    def run(self):
        """Executes the staging workflow."""
        self._run_workflow(self.workflow)

    def _create_workflow(self, scan, base_dir=None):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.

        :param scan: the scan number
        :param base_dir: the workflow execution directory
            (default is a new temp directory)
        :return: the new workflow
        """
        self._logger.debug("Creating the %d DICOM processing workflow..." %
                           scan)

        # The Nipype workflow object.
        workflow = pe.Workflow(name='staging', base_dir=base_dir)

        # The workflow input.
        in_fields = ['collection', 'subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self._logger.debug("The %s workflow input node is %s with fields %s" %
                         (workflow.name, input_spec.name, in_fields))

        # Create the session, if necessary. The gate blocks upload until the
        # session is created.
        find_session_xfc = XNATFind(project=project(), modality='MR', create=True)
        find_session = pe.Node(find_session_xfc, name='find_session')
        workflow.connect(input_spec, 'subject', find_session, 'subject')
        workflow.connect(input_spec, 'session', find_session, 'session')
        session_gate_xfc = Gate(fields=['session', 'xnat_id'])
        session_gate = pe.Node(session_gate_xfc, name='session_gate')
        workflow.connect(input_spec, 'session', session_gate, 'session')
        workflow.connect(find_session, 'xnat_id', session_gate, 'xnat_id')

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
        # Upload the compressed DICOM files.
        upload_dicom_xfc = XNATCopy(project=project(), scan=scan,
                                    resource='DICOM', skip_existing=True)
        upload_dicom = pe.JoinNode(upload_dicom_xfc, joinsource='iter_dicom',
                                   joinfield='in_files', name='upload_dicom')
        workflow.connect(input_spec, 'subject', upload_dicom, 'subject')
        workflow.connect(session_gate, 'session', upload_dicom, 'session')
        workflow.connect(compress_dicom, 'out_file', upload_dicom, 'in_files')

        # Stack the scan volume into a 3D NiFTI file.
        # TODO - obtain the DICOM volume_tag below from the AIRCCollection via
        # a function argument.
        volume_tag = 'AcquisitionNumber'
        out_format = "volume%%(%s)03d" % volume_tag
        stack_xfc = DcmStack(embed_meta=True, out_format=out_format)
        stack = pe.JoinNode(stack_xfc, joinsource='iter_dicom',
                            joinfield='dicom_files', name='stack')

        workflow.connect(fix_dicom, 'out_file', stack, 'dicom_files')

        # TODO - if upload works, then delete the commented lines below and
        # following the line:
        #   workflow.connect(iter_volume, 'volume', upload_3d, 'scan')
        #
        # # Force the T1 3D upload to follow DICOM upload.
        # # Note: XNAT fails app. 80% into the T1 upload. It appears to be a
        # # concurrency conflict, possibly arising from the following causes:
        # # * the non-reentrant pyxnat's custom non-http2lib cache is corrupted
        # # * an XNAT archive directory access race condition
        # #
        # # However, the error cannot be isolated for the following reasons:
        # # * the error is sporadic and unreproducible
        # # * since nipype swallows non-nipype Python log messages, the upload
        # #   and pyxnat log messages disappear
        # #
        # # This gate task serializes upload to prevent potential XNAT access
        # # conflicts.
        # #
        # # TODO - isolate and fix.
        # #
        # if scan == 1:
        #     upload_3d_gate_xfc = Gate(fields=['out_file', 'xnat_files'])
        #     upload_3d_gate = pe.Node(upload_3d_gate_xfc, name='upload_3d_gate')
        #     workflow.connect(upload_dicom, 'xnat_files', upload_3d_gate, 'xnat_files')
        #     workflow.connect(stack, 'out_file', upload_3d_gate, 'out_file')

        # Upload the 3D NiFTI stack files to XNAT.
        upload_3d_xfc = XNATCopy(project=project(), scan=scan,
                                 resource='NIFTI', skip_existing=True)
        upload_3d = pe.Node(upload_3d_xfc, name='upload_3d')
        workflow.connect(input_spec, 'subject', upload_3d, 'subject')
        workflow.connect(session_gate, 'session', upload_3d, 'session')
        # # 3D upload is gated by DICOM upload.
        # workflow.connect(upload_3d_gate, 'out_file', upload_3d, 'in_files')
        workflow.connect(stack, 'out_file', upload_3d, 'in_files')

        # The output is the 3D NiFTI stack file. Make the output a Gate node
        # rather than an IdentityInterface in order to prevent nipype from
        # overzealously pruning it as extraneous.
        output_spec_xfc = Gate(fields=['image', 'xnat_files'])
        output_spec = pe.Node(output_spec_xfc, name='output_spec')
        workflow.connect(stack, 'out_file', output_spec, 'image')
        workflow.connect(upload_3d, 'xnat_files', output_spec, 'xnat_files')

        # Instrument the nodes for cluster submission, if necessary.
        self._configure_nodes(workflow)

        self._logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)

        return workflow
