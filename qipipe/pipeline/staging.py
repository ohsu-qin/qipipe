import os
import glob
import shutil
import itertools
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces.dcmstack import DcmStack
import qixnat
from ..interfaces import (
    StickyIdentityInterface, FixDicom, Compress, XNATFind, XNATUpload
)
from .workflow_base import WorkflowBase
from ..helpers.logging import logger
from ..staging import iterator
from ..staging.sort import sort
from .pipeline_error import PipelineError

SCAN_METADATA_RESOURCE = 'metadata'
"""The label of the XNAT resource holding the scan configuration."""

SCAN_CONF_FILE = 'scan.cfg'
"""The XNAT scan configuration file name."""


def run(subject, session, scan, *in_dirs, **opts):
    """
    Runs the staging workflow on the given DICOM input directory.
    The return value is a {volume: file} dictionary, where *volume*
    is the volume number and *file* is the 3D NIfTI volume file.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_dirs: the input DICOM file directories
    :param opts: the :class:`StagingWorkflow` initializer options
    :return: the compressed 3D volume NIfTI files
    """
    # The target directory for the fixed, compressed DICOM files.
    _logger = logger(__name__)
    dest_opt = opts.pop('dest', None)
    if dest_opt:
        dest = os.path.abspath(dest_opt)
        if not os.path.exists(dest):
            os.makedirs(dest)
    else:
        dest = os.getcwd()

    # Print a debug log message.
    in_dirs_s = in_dirs[0] if len(in_dirs) == 1 else [d for d in in_dirs]
    _logger.debug("Staging the %s %s scan %d files in %s..." %
                  (subject, session, scan, in_dirs_s))

    # We need the collection up front before creating the workflow, so
    # we can't follow the roi or registration idiom of delegating to the
    # workflow constructor to determine the collection.
    coll_opt = opts.pop('collection', None)
    if coll_opt:
        collection = coll_opt
    else:
        parent_wf = opts.get('parent')
        if parent_wf:
            collection = parent_wf.collection
        else:
            raise PipelineError('The staging collection could not be'
                                ' determined from the options')

    # Make the scan workflow.
    scan_wf = ScanStagingWorkflow(**opts)
    # Sort the volumes.
    vol_dcm_dict = sort(collection, scan, *in_dirs)
    # Execute the workflow.
    return scan_wf.run(collection, subject, session, scan, vol_dcm_dict, dest)


class ScanStagingWorkflow(WorkflowBase):
    """
    The ScanStagingWorkflow class builds and executes the scan
    staging supervisory Nipype workflow. This workflow delegates
    to :meth:`qipipe.pipeline.staging.stage_volume` for each
    iterated scan volume.

    The scan staging workflow input is the *input_spec* node
    consisting of the following input fields:

    - *collection*: the collection name

    - *subject*: the subject name

    - *session*: the session name

    - *scan*: the scan number

    The scan staging workflow has one iterable:

    - the *iter_volume* node with input fields *volume* and *in_files*

    This iterable must be set prior to workflow execution.

    The staging workflow output is the *output_spec* node consisting
    of the following output field:

    - *out_file*: the 3D volume stack NIfTI image file
    """

    def __init__(self, **opts):
        """
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer keyword arguments
        """
        super(ScanStagingWorkflow, self).__init__(__name__, **opts)

        # Make the workflow.
        self.workflow = self._create_workflow()
        """
        The scan staging workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """

    def run(self, collection, subject, session, scan, vol_dcm_dict, dest):
        """
        Executes this scan staging workflow.

        :param collection: the collection name
        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param vol_dcm_dict: the input {volume: DICOM files} dictionary
        :param dest: the destination directory
        """
        # Set the top-level inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.dest = dest

        # Prime the volume iterator.
        in_volumes = sorted(vol_dcm_dict.iterkeys())
        dcm_files = [vol_dcm_dict[v] for v in in_volumes]
        iter_dict = dict(volume=in_volumes, in_files=dcm_files)
        iterables = iter_dict.items()
        iter_volume = self.workflow.get_node('iter_volume')
        iter_volume.iterables = iterables
        # Iterate over the volumes and corresponding DICOM files
        # in lock-step.
        iter_volume.synchronize = True

        # Execute the workflow.
        wf_res = self._run_workflow(self.workflow)

        # The magic incantation to get the Nipype workflow result.
        output_res = next(n for n in wf_res.nodes() if n.name == 'output_spec')
        results = output_res.inputs.get()['out_files']

        self.logger.debug(
            "Executed the %s workflow on the %s %s scan %d with 3D volume"
            " results:\n%s" %
            (self.workflow.name, subject, session, scan, results)
        )

        # Return the staged 3D volume files.
        return results

    def _create_workflow(self):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        :return: the new workflow
        """
        self.logger.debug('Creating the scan staging workflow...')

        # The Nipype workflow object.
        workflow = pe.Workflow(name='stage_scan')

        # The workflow input.
        hierarchy_fields = ['subject', 'session', 'scan']
        in_fields = hierarchy_fields + ['collection', 'dest']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self.logger.debug("The %s workflow input node is %s with fields %s" %
                         (workflow.name, input_spec.name, in_fields))

        # The volume iterator.
        iter_fields = ['volume', 'in_files']
        iter_volume = pe.Node(IdentityInterface(fields=iter_fields),
                              name='iter_volume')
        self.logger.debug("The %s workflow volume iterator node is %s"
                          " with fields %s" %
                         (workflow.name, iter_volume.name, iter_fields))

        # The volume staging node wraps the stage_volume function.
        stg_inputs = (
            in_fields + iter_fields + ['collection', 'base_dir', 'opts']
        )
        stg_xfc = Function(input_names=stg_inputs, output_names=['out_dir'],
                           function=stage_volume)
        stg_node = pe.Node(stg_xfc, name='stage')
        child_opts = self._child_options()
        base_dir = child_opts.pop('base_dir')
        stg_node.inputs.base_dir = base_dir
        stg_node.inputs.opts = child_opts
        for fld in in_fields:
            workflow.connect(input_spec, fld, stg_node, fld)
        for fld in iter_fields:
            workflow.connect(iter_volume, fld, stg_node, fld)

        # Upload the processed DICOM and 3D volume NIfTI files.
        # The upload out_files output is the volume files.
        upload_fields = hierarchy_fields + ['project', 'in_dir']
        upload_xfc = Function(input_names=upload_fields,
                              output_names=['out_files'],
                              function=upload)
        upload = pe.Node(upload_xfc, name='upload')
        upload.inputs.project = self.project
        workflow.connect(input_spec, 'subject', upload, 'subject')
        workflow.connect(input_spec, 'session', upload, 'session')
        workflow.connect(input_spec, 'scan', upload, 'scan')
        workflow.connect(input_spec, 'dest', upload, 'in_dir')

        # The output is the 3D NIfTI volume image files.
        output_spec = pe.Node(StickyIdentityInterface(fields=['out_files']),
                              name='output_spec')
        workflow.connect(upload, 'out_files', output_spec, 'out_files')

        # Instrument the nodes for cluster submission, if necessary.
        self._configure_nodes(workflow)

        return workflow


class VolumeStagingWorkflow(WorkflowBase):
    """
    The StagingWorkflow class builds and executes the staging Nipype workflow.
    The staging workflow includes the following steps:

    - Group the input DICOM images into volume.

    - Fix each input DICOM file header using the
      :class:`qipipe.interfaces.fix_dicom.FixDicom` interface.

    - Compress each corrected DICOM file.

    - Upload each compressed DICOM file into XNAT.

    - Stack each new volume's 2-D DICOM files into a 3-D volume NIfTI file
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
      :attr:`qipipe.staging.image_collection.Collection.patterns`
      ``volume`` DICOM tag

    * *file* is the DICOM file name

    The staging workflow output is the *output_spec* node consisting
    of the following output field:

    - *image*: the 3D volume stack NIfTI image file

    Note: Concurrent XNAT upload fails unpredictably due to one of
        the causes described in the ``qixnat.facade.XNAT.find`` method
        documentation.

        The errors are addressed by the following measures:
        * setting an isolated pyxnat cache_dir for each execution node
        * serializing the XNAT find-or-create access points with JoinNodes
        * increasing the SGE submission resource parameters. The following
          setting is adequate:
             h_rt=02:00:00,mf=32M

    .. _CTP: https://wiki.cancerimagingarchive.net/display/Public/Image+Submitter+Site+User%27s+Guide
    .. _DcmStack: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.dcmstack.html
    """

    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer keyword arguments
        """
        super(VolumeStagingWorkflow, self).__init__(__name__, **opts)

        # Make the workflow.
        self.workflow = self._create_workflow()
        """
        The staging workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """

    def run(self, collection, subject, session, scan, volume, dest, *in_files):
        """
        Executes this volume staging workflow.

        :param collection: the collection name
        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param volume: the volume number
        :param dest: the destination directory
        :param in_files: the input DICOM files
        """
        # Set the top-level inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.volume = volume
        input_spec.inputs.dest = dest

        # Set the DICOM file iterator inputs.
        iter_dicom = self.workflow.get_node('iter_dicom')
        iter_dicom.iterables = ('dicom_file', in_files)

        # Execute the workflow.
        wf_res = self._run_workflow(self.workflow)

        # The magic incantation to get the Nipype workflow result.
        output_res = next(n for n in wf_res.nodes() if n.name == 'output_spec')
        result = output_res.inputs.get()['out_file']

        self.logger.debug(
            "Executed the %s workflow on the %s %s scan %d with 3D volume"
            " result %s" % (self.workflow.name, subject, session, scan, result)
        )

        # Return the staged 3D volume files.
        return result

    def _create_workflow(self):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        :return: the new workflow
        """
        self.logger.debug('Creating the DICOM processing workflow...')

        # The Nipype workflow object.
        workflow = pe.Workflow(name='staging', base_dir=self.base_dir)

        # The workflow input.
        in_fields = ['collection', 'subject', 'session', 'scan',
                     'volume', 'dest']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self.logger.debug("The %s workflow input node is %s with fields %s" %
                         (workflow.name, input_spec.name, in_fields))

        # The DICOM file iterator.
        iter_dicom = pe.Node(IdentityInterface(fields=['dicom_file']),
                             name='iter_dicom')
        self.logger.debug("The %s workflow DICOM iterable node is %s." %
                           (workflow.name, iter_dicom.name))

        # Fix the DICOM tags.
        fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
        workflow.connect(input_spec, 'collection', fix_dicom, 'collection')
        workflow.connect(input_spec, 'subject', fix_dicom, 'subject')
        workflow.connect(iter_dicom, 'dicom_file', fix_dicom, 'in_file')

        # Compress the corrected DICOM files.
        compress_dicom = pe.Node(Compress(), name='compress_dicom')
        workflow.connect(fix_dicom, 'out_file', compress_dicom, 'in_file')
        workflow.connect(input_spec, 'dest', compress_dicom, 'dest')

        # The volume file name format.
        vol_fmt_xfc = Function(input_names=['collection'],
                               output_names=['format'],
                               function=volume_format)
        vol_fmt = pe.Node(vol_fmt_xfc, name='volume_format')
        workflow.connect(input_spec, 'collection', vol_fmt, 'collection')

        # Stack the scan slices into a 3D volume NIfTI file.
        stack_xfc = DcmStack(embed_meta=True)
        stack = pe.JoinNode(stack_xfc, joinsource='iter_dicom',
                            joinfield='dicom_files', name='stack')
        workflow.connect(fix_dicom, 'out_file', stack, 'dicom_files')
        workflow.connect(vol_fmt, 'format', stack, 'out_format')

        # The output is the 3D NIfTI stack file.
        output_flds = ['out_file']
        output_xfc = StickyIdentityInterface(fields=output_flds)
        output_spec = pe.Node(output_xfc, name='output_spec')
        workflow.connect(stack, 'out_file', output_spec, 'out_file')

        # Instrument the nodes for cluster submission, if necessary.
        self._configure_nodes(workflow)

        self.logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)

        return workflow


def stage_volume(collection, subject, session, scan, volume, in_files,
                 dest, base_dir, opts):
    """
    Stages the given volume. The processed DICOM ``.dcm.gz`` files
    and merged 3D NIfTI volume ``.nii.gz`` file are placed in the
    *dest*/*volume* subdirectory.

    :param collection: the collection name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param volume: the volume number
    :param in_files: the input DICOM files
    :param dest: the parent destination directory
    :param base_dir: the parent base directory
    :param opts: the non-base_dir :class:`VolumeStagingWorkflow`
        initializer options
    :return: the volume target directory
    """
    import os
    import shutil
    from qipipe.helpers.logging import logger
    from qipipe.pipeline.staging import VolumeStagingWorkflow

    # The destination is a subdirectory.
    out_dir = "%s/volume%03d" % (dest, volume)
    os.mkdir(out_dir)

    # The workflow runs in a subdirectory.
    vol_base_dir = "%s/volume%03d" % (base_dir, volume)
    os.mkdir(vol_base_dir)

    # Make the workflow.
    stg_wf = VolumeStagingWorkflow(base_dir=base_dir, **opts)
    # Execute the workflow.
    logger(__name__).debug("Staging %s %s scan %d volume %d in %s..." %
                           (subject, session, scan, volume, out_dir))
    stg_wf.run(collection, subject, session, scan, volume, out_dir, *in_files)
    logger(__name__).debug("Staged %s %s scan %d volume %d in %s." %
                           (subject, session, scan, volume, out_dir))

    return out_dir


def upload(project, subject, session, scan, in_dir):
    """
    Uploads the staged files in *in_dir* as follows:
    * the processed DICOM ``.dcm.gz`` files are uploaded to the
      XNAT scan ``DICOM`` resource
    * the merged NIfTI ``.nii.gz`` files are uploaded to the
      XNAT scan ``NIFTI`` resource

    :param project: the project name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_dir: the input staged directory
    :return: the 3D volume image NIfTI files
    """
    import glob
    import qixnat
    from qipipe.helpers.logging import logger

    # The DICOM files to upload.
    dcm_files = glob.iglob("%s/volume*/*.dcm.gz" % in_dir)
    # Upload the compressed DICOM files in one action.
    logger(__name__).debug(
        "Uploading the %s %s scan %d staged DICOM files to XNAT..." %
        (subject, session, scan)
    )
    with qixnat.connect() as xnat:
        # The target XNAT scan DICOM resource object.
        # The modality option is required if it is necessary to
        # create the XNAT scan object.
        rsc = xnat.find_or_create(
            project, subject, session, scan=scan, resource='DICOM',
            modality='MR'
        )
        xnat.upload(rsc, *dcm_files)
    _logger.debug("Uploaded the %s %s scan %d staged DICOM files to XNAT." %
                  (subject, session, scan))

    # The NIfTI files to upload.
    nii_files = glob.glob("%s/volume*/*.nii.gz" % in_dir)
    # Upload the NIfTI files in one action.
    _logger.debug("Uploading the %s %s scan %d staged NIfTI files to XNAT..." %
                  (subject, session, scan))
    with qixnat.connect() as xnat:
        # The target XNAT scan NIFTI resource object.
        rsc = xnat.find_or_create(
            project, subject, session, scan=scan, resource='NIFTI'
        )
        xnat.upload(rsc, *nii_files)
    _logger.debug("Uploaded the %s %s scan %d staged NIfTI files to XNAT." %
                  (subject, session, scan))

    return nii_files


def volume_format(collection):
    """
    The DcmStack format for making a file name from the DICOM
    volume tag.

    Example::

        coll = Collection(volume='AcquisitionNumber', ...)
        volume_format(coll)
        >> "volume%(AcquisitionNumber)03d"


    :param collection: the collection name
    :return: the volume file name format
    """
    from qipipe.staging import image_collection

    coll = image_collection.with_name(collection)

    # Escape the leading % and inject the DICOM tag.
    return "volume%%(%s)03d" % coll.patterns.volume
