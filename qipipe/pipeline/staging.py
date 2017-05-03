import os
import glob
import shutil
import itertools
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces.dcmstack import DcmStack
import qixnat
from ..interfaces import (Gate, FixDicom, Compress, XNATFind, XNATUpload)
from .workflow_base import WorkflowBase
from ..helpers.logging import logger
from ..staging import iterator
from ..staging.sort import sort
from .pipeline_error import PipelineError

SCAN_METADATA_RESOURCE = 'metadata'
"""The label of the XNAT resource holding the scan configuration."""

SCAN_CONF_FILE = 'scan.cfg'
"""The XNAT scan configuration file name."""


def run(collection, subject, session, scan, *in_dirs, **opts):
    """
    Runs the staging workflow on the given DICOM input directory.
    The return value is a {volume: file} dictionary, where *volume*
    is the volume number and *file* is the 3D NIfTI volume file.

    :param collection: the collection name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_dirs: the input DICOM file directories
    :param opts: the :class:`StagingWorkflow` initializer options
    :return: the {volume: file} dictionary
    """
    # The target directory for the fixed, compressed DICOM files.
    _logger = logger(__name__)
    dest = opts.get('dest', os.getcwd())
    if not dest:
        dest = os.getcwd()
    elif not os.path.exists(dest):
        os.makedirs(dest)

    in_dirs_s = in_dirs[0] if len(in_dirs) == 1 else [d for d in in_dirs]
    _logger.debug("Staging the %s %s scan %d files in %s..." %
                  (subject, session, scan, in_dirs_s))
    # Sort the volumes.
    vol_dcm_dict = sort(collection, scan, *in_dirs)
    # Stage the volumes.
    vol_dirs = []
    vol_file_dict = {}
    project = None
    for volume, in_files in vol_dcm_dict.iteritems():
        # Put the compressed DICOM files in an empty volume
        # subdirectory.
        vol_dest = "%s/%d" % (dest, volume)
        if os.path.exists(vol_dest):
            shutil.rmtree(vol_dest)
        os.mkdir(vol_dest)
        vol_dirs.append(vol_dest)
        # Make the workflow.
        stg_wf = StagingWorkflow(**opts)
        # Capture the project for later.
        if not project:
            project = stg_wf.project
        # Set the iterables.
        stg_wf.set_inputs(collection, subject, session, scan, volume,
                          vol_dest, *in_files)
        # Run the workflow.
        stg_wf.run()
        # Work around the following Nipype defect:
        # * Nipype does not have a mechanism for collecting workflow
        #   outputs.
        # Look for the 3D volume output in the workflow base directory
        # work area directly instead.
        vol_file = ("%s/staging/stack/volume%03d.nii.gz" %
                    (stg_wf.base_dir, volume))
        if not os.path.exists(vol_file):
            raise PipelineError("Volume file not found: %s" % vol_file)
        vol_file_dict[volume] = vol_file
    _logger.debug("Staged %d volumes in %s." % (len(vol_dcm_dict), dest))
    # The compressed DICOM files. The scan_files must be collected into
    # an array rather than iterated from a generator to work around the
    # following Nipype bug:
    # * Nipype does not support an InputMultiPath generator argument.
    dcm_files = []
    for vol_dir in vol_dirs:
        vol_files = glob.glob("%s/*" % vol_dir)
        dcm_files.extend(vol_files)
    if dcm_files:
        # Upload the compressed DICOM files in one action.
        upload = XNATUpload(project=project, subject=subject,
                            session=session, scan=scan, resource='DICOM',
                            modality='MR', in_files=dcm_files)
        _logger.debug("Uploading the %s %s scan %d staged DICOM files to XNAT..." %
                      (subject, session, scan))
        upload.run()
        _logger.debug("Uploaded the %s %s scan %d staged DICOM files to XNAT." %
                      (subject, session, scan))

    return vol_file_dict


class StagingWorkflow(WorkflowBase):
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
        super(StagingWorkflow, self).__init__(logger=logger(__name__), **opts)

        # Make the workflow.
        self.workflow = self._create_workflow()
        """
        The staging workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """

    def set_inputs(self, collection, subject, session, scan, volume, dest,
                   *in_files):
        """
        Sets the staging workflow inputs for the *input_spec* node
        and the iterables.

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

    def run(self):
        """Executes this staging workflow."""
        self._run_workflow(self.workflow)

    def _create_workflow(self):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        :return: the new workflow
        """
        self.logger.debug('Creating the DICOM processing workflow...')

        # The Nipype workflow object.
        workflow = pe.Workflow(name='staging')

        # The workflow input.
        in_fields = ['collection', 'subject', 'session', 'scan', 'volume', 'dest']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self.logger.debug("The %s workflow input node is %s with fields %s" %
                         (workflow.name, input_spec.name, in_fields))

        # Create the scan, if necessary. The gate blocks upload until the
        # scan is created.
        find_scan_xfc = XNATFind(project=self.project, modality='MR',
                                 create=True)
        find_scan = pe.Node(find_scan_xfc, name='find_scan')
        workflow.connect(input_spec, 'subject', find_scan, 'subject')
        workflow.connect(input_spec, 'session', find_scan, 'session')
        workflow.connect(input_spec, 'scan', find_scan, 'scan')
        scan_gate_xfc = Gate(fields=['scan', 'xnat_id'])
        scan_gate = pe.Node(scan_gate_xfc, name='scan_gate')
        workflow.connect(input_spec, 'scan', scan_gate, 'scan')
        workflow.connect(find_scan, 'xnat_id', scan_gate, 'xnat_id')

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

        # Stack the scan volume into a 3D NIfTI file.
        stack_xfc = DcmStack(embed_meta=True)
        stack = pe.JoinNode(stack_xfc, joinsource='iter_dicom',
                            joinfield='dicom_files', name='stack')
        workflow.connect(fix_dicom, 'out_file', stack, 'dicom_files')
        workflow.connect(vol_fmt, 'format', stack, 'out_format')

        # Upload the 3D NIfTI stack file to XNAT.
        upload_3d_xfc = XNATUpload(project=self.project, resource='NIFTI',
                                   modality='MR')
        upload_3d = pe.Node(upload_3d_xfc, name='upload_3d')
        workflow.connect(input_spec, 'subject', upload_3d, 'subject')
        workflow.connect(input_spec, 'session', upload_3d, 'session')
        workflow.connect(input_spec, 'scan', upload_3d, 'scan')
        workflow.connect(stack, 'out_file', upload_3d, 'in_files')

        # The output is the 3D NIfTI stack file. Make an intermediate Gate
        # node to ensure that upload is completed before setting the output
        # field.
        output_gate_xfc = Gate(fields=['image', 'xnat_files'])
        output_gate = pe.Node(output_gate_xfc, name='output_gate')
        workflow.connect(stack, 'out_file', output_gate, 'image')
        workflow.connect(upload_3d, 'xnat_files', output_gate, 'xnat_files')

        # Make the output a Gate node to work around the following Nipype
        # bug:
        # * Nipype overzealously prunes an IdentityInterface node as
        #   extraneous, even if it is connected in a parent workflow.
        # TODO - verify that this is still the case
        output_spec_xfc = Gate(fields=['image'])
        output_spec = pe.Node(output_spec_xfc, name='output_spec')
        workflow.connect(output_gate, 'image', output_spec, 'image')

        # Instrument the nodes for cluster submission, if necessary.
        self._configure_nodes(workflow)

        self.logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)

        return workflow


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
