import os
import glob
import shutil
import itertools
import logging
# The ReadTheDocs build does not include nipype.
on_rtd = os.environ.get('READTHEDOCS') == 'True'
if not on_rtd:
    # Disable nipype nipy import FutureWarnings.
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        from nipype.pipeline import engine as pe
        from nipype.interfaces.utility import (IdentityInterface, Function)
        from nipype.interfaces.dcmstack import (DcmStack, MergeNifti)
import qixnat
from ..interfaces import (StickyIdentityInterface, FixDicom, Compress)
from .workflow_base import WorkflowBase
from ..helpers.constants import (SCAN_TS_BASE, SCAN_TS_FILE, VOLUME_FILE_PAT)
from ..helpers.logging import logger
from ..staging import (iterator, image_collection)
from ..staging.ohsu import MULTI_VOLUME_SCAN_NUMBERS
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
    :param opts: the :class:`ScanStagingWorkflow` initializer options
    :return: the :meth:`ScanStagingWorkflow.run` result
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
    is_multi_volume = scan in MULTI_VOLUME_SCAN_NUMBERS
    scan_wf = ScanStagingWorkflow(is_multi_volume=is_multi_volume, **opts)
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

    def __init__(self, is_multi_volume=True, **opts):
        """
        :param is_multi_volume: flag indicating whether to include
            volume merge tasks
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer keyword arguments
        """
        super(ScanStagingWorkflow, self).__init__(__name__, **opts)

        # Make the workflow.
        self.workflow = self._create_workflow(is_multi_volume)
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
        :return: the (time series, volume files) tuple
        """
        # Set the top-level inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.dest = dest

        # The volume grouping tag.
        img_coll = image_collection.with_name(collection)
        volume_tag = img_coll.patterns.volume
        if not volume_tag:
            raise PipelineError('The collection configuration DICOM'
                                ' volume tag is missing.')
        input_spec.inputs.volume_tag = volume_tag

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
        # If dry-run, then _run_workflow is a no-op.
        if not wf_res:
            return

        # The magic incantation to get the Nipype workflow result.
        output_res = next(n for n in wf_res.nodes() if n.name == 'output_spec')
        time_series = output_res.inputs.get()['time_series'],
        volume_files = output_res.inputs.get()['volume_files'],

        self.logger.debug(
            "Executed the %s workflow on the %s %s scan %d to create"
            " %d volume files and the 4D time series %s." %
            (self.workflow.name, subject, session, scan,
             len(volume_files), time_series)
        )

        # Return the (time series, volume files) result.
        return time_series, volume_files

    def _create_workflow(self, is_multi_volume=True):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.

        :param is_multi_volume: flag indicating whether to include
            volume merge tasks
        :return: the new workflow
        """
        self.logger.debug('Creating the scan staging workflow...')

        # The Nipype workflow object.
        workflow = pe.Workflow(name='stage_scan', base_dir=self.base_dir)

        # The workflow input.
        hierarchy_fields = ['subject', 'session', 'scan']
        stg_fields = hierarchy_fields + ['collection', 'dest']
        in_fields = stg_fields + ['volume_tag']
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
        stg_inputs = stg_fields + iter_fields + ['opts']
        stg_xfc = Function(input_names=stg_inputs, output_names=['out_file'],
                           function=stage_volume)
        stg_node = pe.Node(stg_xfc, name='stage_volume')
        stg_node.inputs.opts = self._child_options()
        for fld in stg_fields:
            workflow.connect(input_spec, fld, stg_node, fld)
        for fld in iter_fields:
            workflow.connect(iter_volume, fld, stg_node, fld)

        # Collect the 3D volume files.
        collect_xfc = IdentityInterface(fields=['volume_files'])
        collect_vols = pe.JoinNode(
            collect_xfc, joinsource='iter_volume',
            joinfield='volume_files', name='collect_volumes'
        )
        workflow.connect(stg_node, 'out_file', collect_vols, 'volume_files')

        # Upload the processed DICOM and NIfTI files.
        # The upload out_files output is the volume files.
        upload_fields = (
            hierarchy_fields +
            ['project', 'dcm_dir', 'volume_files', 'time_series']
        )
        upload_xfc = Function(input_names=upload_fields,
                              output_names=['out_files'],
                              function=upload)
        upload_node = pe.Node(upload_xfc, name='upload')
        upload_node.inputs.project = self.project
        workflow.connect(input_spec, 'subject', upload_node, 'subject')
        workflow.connect(input_spec, 'session', upload_node, 'session')
        workflow.connect(input_spec, 'scan', upload_node, 'scan')
        workflow.connect(input_spec, 'dest', upload_node, 'dcm_dir')
        workflow.connect(collect_vols, 'volume_files',
                         upload_node, 'volume_files')
        if is_multi_volume:
            # Merge the volumes.
            merge_vols_xfc = MergeNifti(out_format=SCAN_TS_BASE)
            merge_vols = pe.Node(merge_vols_xfc, name='merge_volumes')
            workflow.connect(input_spec, 'volume_tag',
                             merge_vols, 'sort_order')
            workflow.connect(collect_vols, 'volume_files',
                             merge_vols, 'in_files')
            workflow.connect(merge_vols, 'out_file',
                             upload_node, 'time_series')
            self.logger.debug('Connected staging to scan time series merge.')
        else:
            upload_node.inputs.time_series = None
        self.logger.debug('Connected scan time series merge to upload.')

        # The output is the 4D time series and 3D NIfTI volume image files.
        output_fields = ['time_series', 'volume_files']
        output_spec = pe.Node(StickyIdentityInterface(fields=output_fields),
                              name='output_spec')
        workflow.connect(collect_vols, 'volume_files',
                         output_spec, 'volume_files')
        if is_multi_volume:
            workflow.connect(merge_vols, 'out_file', output_spec, 'time_series')
        else:
            output_spec.inputs.time_series = None

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

    - *subject* is the subject name, e.g. ``Breast011``

    - *session* is the session name, e.g. ``Session03``

    * *volume number* is determined by the
      :attr:`qipipe.staging.image_collection.Collection.patterns`
      ``volume`` DICOM tag

    - *file* is the DICOM file name

    The staging workflow output is the *output_spec* node consisting
    of the following output field:

    * _image_: the 3D volume stack NIfTI image file

    Note: Concurrent XNAT upload fails unpredictably due to one of
        the causes described in the ``qixnat.facade.XNAT.find`` method
        documentation.

        The errors are addressed by the following measures:
        * setting an isolated pyxnat cache_dir for each execution node
        * serializing the XNAT find-or-create access points with JoinNodes
        * increasing the SGE submission resource parameters as shown in
          the ``conf/staging.cfg [upload]`` section

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

    def run(self, collection, subject, session, scan, volume, dest,
            *in_files):
        """
        Executes this volume staging workflow.

        :param collection: the collection name
        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param volume: the volume number
        :param dest: the destination directory
        :param in_files: the input DICOM files
        :return: the output 3D NIfTI volume file path
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
        output_res = next(
            n for n in wf_res.nodes() if n.name == 'output_spec'
        )
        out_file = output_res.inputs.get()['out_file']

        self.logger.debug(
            "Executed the %s workflow on the %s %s scan %d with 3D"
            " volume result %s." %
            (self.workflow.name, subject, session, scan, out_file)
        )

        # Return the staged 3D volume files.
        return out_file

    def _create_workflow(self):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        :return: the new workflow
        """
        self.logger.debug('Creating the DICOM processing workflow...')

        # The Nipype workflow object.
        workflow = pe.Workflow(name='stage_volume', base_dir=self.base_dir)

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
                 dest, opts):
    """
    Stages the given volume. The processed DICOM ``.dcm.gz`` files
    are placed in the *dest*/*volume* subdirectory. The child
    :class:`VolumeStagingWorkflow` runs in the
    _parent_/volume\ _volume_ directory, where:
    * _parent_ is the parent base directory specified in the
      options (default current directory)
    * _volume_ is the volume argument

    :param collection: the collection name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param volume: the volume number
    :param in_files: the input DICOM files
    :param dest: the parent destination directory
    :param opts: the :class:`VolumeStagingWorkflow` initializer
         options
    :return: the 3D NIfTI volume file
    """
    import os
    import shutil
    from qipipe.helpers.logging import logger
    from qipipe.pipeline.staging import VolumeStagingWorkflow

    _logger = logger(__name__)
    # The volume destination is a dest subdirectory.
    dest = os.path.abspath(dest)
    out_dir = "%s/volume%03d" % (dest, volume)
    os.mkdir(out_dir)

    # The volume workflow runs in a subdirectory.
    parent_dir = opts.pop('base_dir', os.getcwd())
    base_dir = "%s/volume%03d" % (parent_dir, volume)

    # Make the workflow.
    stg_wf = VolumeStagingWorkflow(base_dir=base_dir, **opts)
    # Execute the workflow.
    logger(__name__).debug("Staging %s %s scan %d volume %d in %s..." %
                           (subject, session, scan, volume, out_dir))
    out_file = stg_wf.run(collection, subject, session, scan, volume,
                          out_dir, *in_files)
    logger(__name__).debug("Staged %s %s scan %d volume %d in %s." %
                           (subject, session, scan, volume, out_dir))

    return out_file


def upload(project, subject, session, scan, dcm_dir, volume_files,
           time_series=None):
    """
    Uploads the staged files in *dcm_dir* as follows:
    * the processed DICOM ``.dcm.gz`` files are uploaded to the
      XNAT scan ``DICOM`` resource
    * the time series and the 3D volume NIfTI ``.nii.gz`` files
      are uploaded to the XNAT scan ``NIFTI`` resource

    :param project: the project name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param dcm_dir: the input staged directory
    :param volume_files: the 3D scan volume files
    :param time_series: the 4D scan time series file, if the scan is
        multi-volume
    :return: the 3D volume image NIfTI files
    """
    import os
    import glob
    import qixnat
    from qipipe.helpers.logging import logger
    from qipipe.pipeline.pipeline_error import PipelineError

    _logger = logger(__name__)
    # The volume directories.
    vol_dir_pat = "%s/volume*" % dcm_dir
    vol_dirs = glob.glob(vol_dir_pat)
    if not vol_dirs:
        raise PipelineError("The input directory does not contain any"
                            " directories matching %s" % vol_dir_pat)
    _logger.debug("Uploading %d %s %s scan %d volumes to XNAT..." %
                  (len(vol_dirs), subject, session, scan))
    # Upload one volume directory at a time, since the DICOM
    # file paths will take up a big chunk of memory.
    dcm_file_cnt = 0
    for vol_dir in vol_dirs:
        # The DICOM files to upload.
        dcm_file_pat = "%s/volume*/*.dcm.gz" % dcm_dir
        dcm_files = glob.glob(dcm_file_pat)
        if not dcm_files:
            raise PipelineError("The input directory does not contain scan"
                                " DICOM files matching %s" % dcm_file_pat)
        # Upload the compressed DICOM files.
        with qixnat.connect() as xnat:
            # The target XNAT scan DICOM resource object.
            # The modality option is required if it is necessary to
            # create the XNAT scan object.
            rsc = xnat.find_or_create(
                project, subject, session, scan=scan, resource='DICOM',
                modality='MR'
            )
            xnat.upload(rsc, *dcm_files)
        dcm_file_cnt += len(dcm_files)
    _logger.debug("Uploaded %d %s %s scan %d staged DICOM files to"
                  " XNAT." % (dcm_file_cnt, subject, session, scan))

    # Upload the NIfTI files in one action.
    nii_file_cnt = len(volume_files) + 1
    _logger.debug("Uploading %d %s %s scan %d staged NIfTI files to"
                  " XNAT..." % (nii_file_cnt, subject, session, scan))
    with qixnat.connect() as xnat:
        # The target XNAT scan NIFTI resource object.
        rsc = xnat.find_or_create(
            project, subject, session, scan=scan, resource='NIFTI'
        )
        if time_series:
            nii_files = volume_files + [time_series]
        else:
            nii_files = volume_files
        xnat.upload(rsc, *nii_files)
    _logger.debug("Uploaded %d %s %s scan %d staged NIfTI files to"
                  " XNAT." % (nii_file_cnt, subject, session, scan))

    return vol_files


def volume_format(collection):
    """
    The DcmStack format for making a file name from the DICOM
    volume tag.

    Example::

        >> volume_format('Sarcoma')
        "volume%(AcquisitionNumber)03d"


    :param collection: the collection name
    :return: the volume file name format
    """
    from qipipe.staging import image_collection

    img_coll = image_collection.with_name(collection)

    # Escape the leading % and inject the DICOM tag.
    return "volume%%(%s)03d" % img_coll.patterns.volume
