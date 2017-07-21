import os
import re
import glob
import shutil
import tempfile
import logging
import six
from bunch import Bunch
# The ReadTheDocs build does not include nipype.
on_rtd = os.environ.get('READTHEDOCS') == 'True'
if not on_rtd:
    # Disable nipype nipy import FutureWarnings.
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        from nipype.pipeline import engine as pe
        from nipype.interfaces.utility import (IdentityInterface, Function, Merge)
import qixnat
from qixnat.helpers import path_hierarchy
from ..helpers.logging import logger
from . import (staging, registration, modeling)
from .pipeline_error import PipelineError
from .workflow_base import WorkflowBase
from . import staging
from ..staging import image_collection
from ..staging.iterator import iter_stage
from ..staging.map_ctp import map_ctp
from ..staging.ohsu import MULTI_VOLUME_SCAN_NUMBERS
from ..staging.roi import (iter_roi, LesionROI)
from ..helpers.constants import (
    SCAN_TS_BASE, SCAN_TS_FILE, VOLUME_FILE_PAT, MASK_RESOURCE, MASK_FILE
)
from ..interfaces import (XNATDownload, XNATUpload)

SINGLE_VOLUME_ACTIONS = ['stage']
"""The workflow actions which apply to a single-volume scan."""

MULTI_VOLUME_ACTIONS = (SINGLE_VOLUME_ACTIONS +
                        ['roi', 'register', 'model'])
"""The workflow actions which apply to a multi-volume scan."""


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
    and runs it on the given inputs. The pipeline execution depends
    on the *actions* option, as follows:

    - If the workflow actions includes ``stage`` or ``roi``, then
      the input is the :meth:`QIPipelineWorkflow.run_with_dicom_input`
      DICOM subject directories input.

    - Otherwise, the input is the
      :meth:`QIPipelineWorkflow.run_with_scan_download` XNAT session
      labels input.

    :param inputs: the input directories or XNAT session labels to
        process
    :param opts: the :meth:`qipipe.staging.iterator.iter_stage`
        and :class:`QIPipelineWorkflow` initializer options,
        as well as the following keyword options:
    :keyword project: the XNAT project name
    :keyword collection: the image collection name
    :keyword actions: the workflow actions to perform
        (default :const:`MULTI_VOLUME_ACTIONS`)
    """
    # The actions to perform.
    actions = opts.pop('actions', MULTI_VOLUME_ACTIONS)
    if 'stage' in actions:
        # Run with staging DICOM subject directory input.
        _run_with_dicom_input(actions, *inputs, **opts)
    elif 'roi' in actions:
        # The non-staging ROI action must be performed alone.
        if len(actions) > 1:
            raise ValueError('The ROI pipeline can only be run'
                             ' with staging or stand-alone')
        _run_with_dicom_input(actions, *inputs, **opts)
    else:
        # Run downstream actions with XNAT session input.
        _run_with_xnat_input(actions, *inputs, **opts)


def _run_with_dicom_input(actions, *inputs, **opts):
    """
    :param actions: the actions to perform
    :param inputs: the DICOM directories to process
    :param opts: the staging iteration and
        `qipipe.pipeline.QIPipelineWorkflow` creation and run options
    """
    # The required XNAT project name.
    project = opts.pop('project', None)
    if not project:
        raise PipelineError('The staging pipeline project option'
                            ' is missing.')

    # The required image collection name.
    collection = opts.pop('collection', None)
    if not collection:
        raise PipelineError('The staging pipeline collection option'
                            ' is missing.')

    # The absolute destination path.
    dest_opt = opts.pop('dest', None)
    if dest_opt:
        dest = os.path.abspath(dest_opt)
    else:
        dest = os.getcwd()

    # The parent base directory. Each scan workflow runs in
    # its own subdirectory. If that were not the case, then
    # a succeeding scan workflow would overwrite its
    # preceding scan workflow's base directory and garble
    # the results.
    base_dir_opt = opts.pop('base_dir', None)
    if base_dir_opt:
        base_dir = os.path.abspath(base_dir_opt)
    else:
        base_dir = os.getcwd()

    # The set of input subjects is used to build the CTP mapping file
    # after the workflow is completed, if staging is enabled.
    subjects = set()
    # Run the workflow on each session and scan.
    # If the only action is ROI, then the input session directories
    # have already been staged. Therefore, set the skip_existing
    # flag to False.
    iter_opts = {}
    scan_opt = opts.pop('scan', None)
    if scan_opt:
        iter_opts['scan'] = scan_opt
    if actions == ['roi']:
        iter_opts['skip_existing'] = False
    actions = set(actions)
    for scan_input in iter_stage(project, collection, *inputs, **iter_opts):
        # Pre-filter the ROI action.
        if 'roi' in actions and scan_input.scan in MULTI_VOLUME_SCAN_NUMBERS:
            roi_files = _collect_roi_files(collection, scan_input)
            if roi_files:
                wf_actions = actions
                wf_opts = opts.copy()
                wf_opts['roi_files'] = roi_files
            else:
                wf_actions = actions - {'roi'}
                wf_opts = opts
        # Further filter the actions.
        wf_actions = _filter_actions(collection, scan_input, wf_actions)
        if not wf_actions:
            continue
        # Capture the subject.
        subjects.add(scan_input.subject)
        # The scan workflow base directory.
        scan_base_dir = "%s/scan/%d" % (base_dir, scan_input.scan)
        # The scan workflow base directory.
        scan_dest = "%s/scan/%d" % (dest, scan_input.scan)
        # Create a new workflow.
        workflow = QIPipelineWorkflow(
            project, scan_input, wf_actions, collection=collection,
            dest=scan_dest, base_dir=scan_base_dir, **wf_opts)
        # Run the workflow on the scan.
        workflow.run_with_dicom_input(wf_actions, scan_input)

    # If staging is enabled, then make the TCIA subject map.
    if 'stage' in actions:
        map_ctp(collection, *subjects, dest=dest)


def _filter_actions(collection, scan_input, actions):
    """
    Filters the specified actions for the given scan input.
    If the scan number is in the :const:`MULTI_VOLUME_SCAN_NUMBERS`,
    then this method returns the specified actions. Otherwise,
    this method returns the actions allowed as
    :const:`SINGLE_VOLUME_ACTIONS`.

    :param actions: the specified actions
    :param scan_input: the :meth:`qipipe.staging.iterator.iter_stage`
        scan input
    :param actions: the specified actions
    :return: the allowed actions
    """
    if scan_input.scan in MULTI_VOLUME_SCAN_NUMBERS:
        return actions
    actions = set(actions)
    allowed = actions.intersection(SINGLE_VOLUME_ACTIONS)
    disallowed = actions.difference(SINGLE_VOLUME_ACTIONS)
    if not allowed:
        logger(__name__).debug(
            "Skipping the %s %s scan %d, since the scan is a"
            " single-volume scan and only the actions %s are"
            " supported for a single-volume scan." %
            (scan_input.subject, scan_input.session,
             scan_input.scan, SINGLE_VOLUME_ACTIONS)
        )
    elif disallowed:
        logger(__name__).debug(
            "Ignoring the %s %s scan %d actions %s, since the scan"
            " is a single-volume scan and only the actions %s are"
            " supported for a single-volume scan." %
            (scan_input.subject, scan_input.session,
             scan_input.scan, disallowed, SINGLE_VOLUME_ACTIONS)
        )

    return allowed


def _run_with_xnat_input(actions, *inputs, **opts):
    """
    Run the pipeline with a XNAT download. Each input is a XNAT scan
    path, e.g. ``/QIN/Breast012/Session03/scan/1``.

    :param actions: the actions to perform
    :param inputs: the XNAT scan resource paths
    :param opts: the :class:`QIPipelineWorkflow` initializer options
    """
    for path in inputs:
        hierarchy = dict(path_hierarchy(path))
        prj = hierarchy.pop('project', None)
        if not prj:
            raise PipelineError("The XNAT path is missing a project: %s" % path)
        # There might be a --pipeline command option.
        # If so, it is either extraneous or conflicting.
        prj_opt = opts.pop('project', None)
        if prj_opt and prj_opt != prj:
            raise PipelineError("The --project option %s conflicts with the"
                                " XNAT path project %s" % (prj_opt, prj))
        sbj = hierarchy.pop('subject', None)
        if not sbj:
            raise PipelineError("The XNAT path is missing a subject: %s" % path)
        sess = hierarchy.pop('experiment', None)
        if not sess:
            raise PipelineError("The XNAT path is missing a session: %s" % path)
        scan_s = hierarchy.pop('scan', None)
        if not scan_s:
            raise PipelineError("The XNAT path is missing a scan: %s" % path)
        scan = int(scan_s)

        # Fashion a scan_input from the hierarchy.
        scan_input = Bunch(subject=sbj, session=sess, scan=scan)
        # Make the workflow.
        workflow = QIPipelineWorkflow(prj, scan_input, actions, **opts)
        # Run the workflow.
        workflow.run_with_scan_download(prj, scan_input, actions)



def _scan_file_exists(xnat, project, scan_input, resource, file_pat=None):
    """
    :param file_pat: the optional target XNAT label pattern to match
        (default any file)
    :return: whether the given XNAT scan file exists
    """
    matches = _scan_files(xnat, project, scan_input, resource, file_pat)

    return not not matches

def _scan_files(xnat, project, scan_input, resource, file_pat=None):
    """
    :param file_pat: the optional target XNAT file label pattern to match
        (default any file)
    :return: the XNAT scan file name list
    """
    rsc_obj = xnat.find_one(project, scan_input.subject, scan_input.session,
                            scan=scan_input.scan, resource=resource)
    if not rsc_obj:
        return []
    # The resource files labels.
    files = rsc_obj.files().get()
    # Filter the files for the match pattern, if necessary.
    if file_pat:
        if isinstance(file_pat, six.string_types):
            file_pat = re.compile(file_pat)
        matches = [f for f in files if file_pat.match(f)]
        logger(__name__).debug(
            "The %s %s %s scan %d resource %s contains %d files matching %s." %
            (project, scan_input.subject, scan_input.session, scan_input.scan,
             resource, len(matches), file_pat.pattern)
        )
        return matches
    else:
        logger(__name__).debug(
            "The %s %s %s scan %d resource %s contains %d files." %
            (project, scan_input.subject, scan_input.session, scan_input.scan,
             resource, len(files))
        )
        return files


class PipelineError(Exception):
    pass


class NotFoundError(Exception):
    pass


class QIPipelineWorkflow(WorkflowBase):
    """
    QIPipeline builds and executes the imaging workflows. The pipeline
    builds a composite workflow which stitches together the following
    constituent workflows:

    - staging: Prepare the new DICOM visits, as described in
      :class:`qipipe.pipeline.staging.StagingWorkflow`

    - mask: Create the mask from the staged images,
      as described in :class:`qipipe.pipeline.mask.MaskWorkflow`

    - registration: Mask, register and realign the staged images,
      as described in
      :class:`qipipe.pipeline.registration.RegistrationWorkflow`

    - modeling: Perform PK modeling as described in
      :class:`qipipe.pipeline.modeling.ModelingWorkflow`

    The constituent workflows are determined by the initialization
    options ``stage``, ``register`` and ``model``. The default is
    to perform each of these subworkflows.

    The workflow steps are determined by the input options as follows:

    - If staging is enabled, then the DICOM files are staged for the
      subject directory inputs. Otherwise, staging is not performed.
      In that case, if registration is enabled as described below, then
      the previously staged volume scan stack images are downloaded.

    - If modeling is enabled and the ``registration`` resource
      option is set, then the previously realigned images with the
      given resource name are downloaded.

    - If registration or modeling is enabled and the XNAT ``mask``
      resource is found, then that resource file is downloaded.
      Otherwise, the mask is created from the staged images.

    The workflow input node is *input_spec* with the following
    fields:

    - *subject*: the subject name

    - *session*: the session name

    - *scan*: the scan number

    The constituent workflows are combined as follows:

    - The staging workflow input is the workflow input.

    - The mask workflow input is the newly created or previously staged
      scan NIfTI image files.

    - The modeling workflow input is the combination of the previously
      uploaded and newly realigned image files.

    The pipeline workflow is available as the
    :attr:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.workflow`
    instance variable.
    """

    def __init__(self, project, scan_input, actions, **opts):
        """
        :param project: the XNAT project name
        :param scan_input: the :meth:`qipipe.staging.iterator.iter_stage`
            scan input
        :param actions: the actions to perform
        :param opts: the :class:`qipipe.staging.WorkflowBase`
            initialization options as well as the following keyword arguments:
        :keyword dest: the staging destination directory
        :keyword collection: the image collection name
        :keyword registration_resource: the XNAT registration resource
            name
        :keyword registration_technique: the
            class:`qipipe.pipeline.registration.RegistrationWorkflow`
            technique
        :keyword modeling_resource: the modeling resource name
        :keyword modeling_technique: the
            class:`qipipe.pipeline.modeling.ModelingWorkflow` technique
        :keyword scan_time_series: the scan time series resource name
        :keyword realigned_time_series: the registered time series resource
            name
        """
        super(QIPipelineWorkflow, self).__init__(
            __name__, project=project, **opts
        )

        coll_opt = opts.pop('collection', None)
        if coll_opt:
            self.collection = image_collection.with_name(coll_opt)
        else:
            self.collection = None

        self.roi_files = opts.pop('roi_files', None)

        # Capture the registration resource name, or generate if
        # necessary. The registration resource name is created
        # here rather than by the registration workflow, since
        # it the registration time series is built and uploaded
        # in the supervisory workflow.
        self.registration_resource = opts.pop('registration_resource', None)
        """The registration resource name."""

        reg_tech_opt = opts.pop('registration_technique', None)
        reg_tech = reg_tech_opt.lower() if reg_tech_opt else None
        if 'register' in actions and not reg_tech:
            raise PipelineError('The registration technique was not'
                                ' specified.')
        self.registration_technique = reg_tech
        """The registration technique."""

        self.modeling_resource = opts.pop('modeling_resource', None)
        """The modeling XNAT resource name."""

        mdl_tech_opt = opts.pop('modeling_technique', None)
        mdl_tech = mdl_tech_opt.lower() if mdl_tech_opt else None
        self.modeling_technique = mdl_tech
        """The modeling technique."""

        if 'model' in actions and not self.modeling_technique:
            raise PipelineError('The modeling technique was not specified.')

        self.workflow = self._create_workflow(scan_input, actions, **opts)
        """
        The pipeline execution workflow. The execution workflow is executed
        by calling the :meth:`run_with_dicom_input` or
        :meth:`run_with_scan_download` method.
        """

    def run_with_dicom_input(self, actions, scan_input):
        """
        :param actions: the workflow actions to perform
        :param scan_input: the :meth:`qipipe.staging.iterator.iter_stage`
            scan input
        :param dest: the TCIA staging destination directory (default is
            the current working directory)
        """
        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.collection = self.collection.name
        input_spec.inputs.subject = scan_input.subject
        input_spec.inputs.session = scan_input.session
        input_spec.inputs.scan = scan_input.scan
        input_spec.inputs.in_dirs = scan_input.dicom

        # Execute the workflow.
        self.logger.info("Running the pipeline on %s %s scan %d." %
                           (scan_input.subject, scan_input.session,
                            scan_input.scan))
        self._run_workflow()
        self.logger.info("Completed pipeline execution on %s %s scan %d." %
                           (scan_input.subject, scan_input.session,
                            scan_input.scan))

    def run_with_scan_download(self, project, scan_input, actions):
        """
        Runs the execution workflow on downloaded scan image files.

        :param project: the project name
        :param scan_input: the {project, subject, session} object
        :param actions: the workflow actions
        """
        self.logger.debug("Processing the %s %s %s scan %d volumes..." %
                           (scan_input.project, scan_input.subject,
                            scan_input.session, scan))
        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = scan_input.subject
        input_spec.inputs.session = scan_input.session
        input_spec.inputs.scan = scan_input.scan

        # Execute the workflow.
        self._run_workflow()

    def _create_workflow(self, scan_input, actions, **opts):
        """
        Builds the reusable pipeline workflow described in
        :class:`qipipe.pipeline.qipipeline.QIPipeline`.

        :param scan_input: the :meth:`qipipe.staging.iterator.iter_stage`
            scan input
        :param actions: the actions to perform
        :param opts: the constituent workflow initializer options
        :return: the Nipype workflow
        """
        # This is a long method body with the following stages:
        #
        # 1. Gather the options.
        # 2. Create the constituent workflows.
        # 3. Tie together the constituent workflows.
        #
        # The constituent workflows are created in back-to-front order,
        # i.e. modeling, registration, mask, roi, staging.
        # This order makes it easier to determine whether to create
        # an upstream workflow depending on the presence of downstream
        # workflows, e.g. the mask is not created if registration
        # is not performed.
        #
        # By contrast, the workflows are tied together in front-to-back
        # order.
        #
        # TODO - Make a qiprofile update stage. Each other stage
        # flows into the update. E.g. take the overall and ROI FSL mean
        # intensity values for the modeling output files.
        #
        self.logger.debug("Building the pipeline execution workflow"
                            " for the actions %s..." % actions)
        # The execution workflow.
        exec_wf = pe.Workflow(name='qipipeline', base_dir=self.base_dir)

        if 'model' in actions:
            mdl_flds = ['subject', 'session', 'scan', 'time_series',
                        'mask', 'bolus_arrival', 'opts']
            mdl_xfc = Function(input_names=mdl_flds,
                               output_names=['results'],
                               function=_model)
            model = pe.Node(mdl_xfc, name='model')
            mdl_opts = self._child_options()
            mdl_opts['technique'] = self.modeling_technique
            model.inputs.opts = mdl_opts
            self.logger.info("Enabled modeling with options %s." % mdl_opts)
        else:
            model = None

        # The registration workflow node.
        if 'register' in actions:
            reg_inputs = ['subject', 'session','scan', 'reference', 'mask',
                          'in_files', 'opts']

            # Spell out the registration workflow options rather
            # than delegating to this qipipeline workflow as the
            # parent, since Nipype Function arguments must be
            # primitive.
            reg_opts = self._child_options()
            reg_opts['technique'] = self.registration_technique
            # The registration function.
            reg_xfc = Function(input_names=reg_inputs,
                               output_names=['time_series'],
                               function=_register)
            register = pe.Node(reg_xfc, name='register')
            register.inputs.opts = reg_opts
            # The fixed reference volume number option.
            reg_ref_opt = opts.pop('registration_reference', None)
            if reg_ref_opt:
                register.inputs.reference = int(reg_ref_opt)
            self.logger.info("Enabled registration with options %s." %
                             reg_opts)
        else:
            self.logger.info("Skipping registration.")
            register = None

        # The ROI workflow node.
        if 'roi' in actions:
            roi_flds = ['subject', 'session', 'scan', 'time_series',
                          'in_rois', 'opts']
            roi_xfc = Function(input_names=roi_flds,
                               output_names=['volume'],
                               function=_roi)
            roi = pe.Node(roi_xfc, name='roi')
            roi.inputs.in_rois = self.roi_files
            roi_opts = self._child_options()
            roi.inputs.opts = roi_opts
            self.logger.info("Enabled ROI conversion with options %s." %
                             roi_opts)
        else:
            roi = None
            self.logger.info("Skipping ROI conversion.")

        # The staging workflow.
        if 'stage' in actions:
            stg_inputs = ['subject', 'session', 'scan', 'in_dirs', 'opts']
            stg_xfc = Function(input_names=stg_inputs,
                               output_names=['time_series', 'volume_files'],
                               function=_stage)
            stage = pe.Node(stg_xfc, name='stage')
            # It would be preferable to pass this QIPipelineWorkflow
            # in the *parent* option, but that induces the following
            # Nipype bug:
            # * A node input which includes a compiled regex results
            #   in the Nipype run error:
            #     TypeError: cannot deepcopy this pattern object
            # The work-around is to break out the separate simple options
            # that the WorkflowBase constructor extracts from the parent.
            stg_opts = self._child_options()
            if 'dest' in opts:
                stg_opts['dest'] = opts['dest']
            if not self.collection:
                raise PipelineError("Staging requires the collection option")
            stg_opts['collection'] = self.collection.name
            stage.inputs.opts = stg_opts
            self.logger.info("Enabled staging with options %s" % stg_opts)
        else:
            stage = None
            self.logger.info("Skipping staging.")

        # Validate that there is at least one constituent workflow.
        if not any([stage, roi, register, model]):
            raise PipelineError("No workflow was enabled.")

        # Registration and modeling require a mask.
        is_mask_required = (
            (register and self.registration_technique != 'Mock') or
            (model and self.modeling_technique != 'Mock')
        )
        if is_mask_required:
            has_mask = False
            # If volumes are already staged, then check for an
            # existing XNAT mask.
            if not stage:
                with qixnat.connect() as xnat:
                    has_mask = _scan_file_exists(
                        xnat, self.project, scan_input, MASK_RESOURCE,
                        MASK_FILE
                    )
            if has_mask:
                dl_mask_xfc = XNATDownload(project=self.project,
                                           resource=MASK_RESOURCE,
                                           file=MASK_FILE)
                mask = pe.Node(dl_mask_xfc, name='download_mask')
            else:
                if not self.collection:
                    raise PipelineError("The mask workflow requires the"
                                        " collection option")
                crop_posterior = self.collection.crop_posterior
                mask_opts = self._child_options()
                mask_opts['crop_posterior'] = crop_posterior
                mask_inputs = ['subject', 'session', 'scan', 'time_series',
                               'opts']
                mask_xfc = Function(input_names=mask_inputs,
                                    output_names=['out_file'],
                                    function=_mask)
                mask = pe.Node(mask_xfc, name='mask')
                mask.inputs.opts = mask_opts
                self.logger.info("Enabled scan mask creation with options"
                                 " %s." % mask_opts)
        else:
            mask = None
            self.logger.info("Skipping scan mask creation.")

        # The workflow input fields.
        input_fields = ['subject', 'session', 'scan']
        # The staging workflow has the additional in_dir input field.
        if stage:
            input_fields.extend(['in_dirs'])

        # The workflow input node.
        input_spec_xfc = IdentityInterface(fields=input_fields)
        input_spec = pe.Node(input_spec_xfc, name='input_spec')
        # Staging, registration, and mask require a volume iterator node.
        # Modeling requires a volume iterator node if and only if
        # modeling is performed on the scan and the scan time series
        # is not available.

        ###
        ### Stitch together the workflows:
        ###

        # If staging is enabled, then stage the DICOM input.
        if stage:
            for field in input_spec.inputs.copyable_trait_names():
                exec_wf.connect(input_spec, field, stage, field)

        # The mask, ROI and scan modeling downstream actions require
        # a scan time series. If there is a scan time series resource
        # option, then the scan time series will be downloaded.
        # Otherwise, it will be created from the staged input.
        is_scan_modeling = (
            model and not register and not self.registration_resource
        )
        need_scan_ts = mask or roi or is_scan_modeling
        if need_scan_ts:
            if stage:
                scan_ts = stage
            else:
                # Validate that there is a XNAT scan time series.
                with qixnat.connect() as xnat:
                    has_scan_ts = _scan_file_exists(
                        xnat, self.project, scan_input, 'NIFTI', SCAN_TS_FILE
                    )
                if not has_scan_ts:
                    raise PipelineError(
                        "The %s %s scan %d NIFTI resource does not include"
                        " the time series file %s" %
                        (scan_input.subject, scan_input.session,
                         scan_input.scan, SCAN_TS_FILE)
                    )
                dl_scan_ts_xfc = XNATDownload(
                    project=self.project, resource='NIFTI', file=SCAN_TS_FILE
                )
                dl_scan_ts = pe.Node(dl_scan_ts_xfc,
                                     name='download_scan_time_series')
                exec_wf.connect(input_spec, 'subject', dl_scan_ts, 'subject')
                exec_wf.connect(input_spec, 'session', dl_scan_ts, 'session')
                exec_wf.connect(input_spec, 'scan', dl_scan_ts, 'scan')
                # Rename the download out_file field to time_series.
                scan_ts_xfc = IdentityInterface(fields=['time_series'])
                scan_ts = pe.Node(scan_ts_xfc, name='scan_time_series')
                exec_wf.connect(dl_scan_ts, 'out_file', scan_ts, 'time_series')

        # Registration and the scan time series require a staged
        # node scan with output 'images'. If staging is enabled,
        # then staged is the stage. Otherwise, the staged node
        # downloads the previously uploaded scan volumes.
        #
        # The scan time series is required by mask and scan
        # registration.
        scan_volumes = None
        if register:
            if stage:
                scan_volumes = stage
            else:
                dl_vols_xfc = XNATDownload(project=self.project,
                                           resource='NIFTI',
                                           file='volume*.nii.gz')
                dl_vols = pe.Node(dl_vols_xfc, name='download_scan_volumes')
                exec_wf.connect(input_spec, 'subject', dl_vols, 'subject')
                exec_wf.connect(input_spec, 'session', dl_vols, 'session')
                exec_wf.connect(input_spec, 'scan', dl_vols, 'scan')
                # Rename the download out_files field to volume_files.
                scan_volumes_xfc = IdentityInterface(fields=['volume_files'])
                scan_volumes = pe.Node(scan_volumes_xfc, name='scan_volumes')
                exec_wf.connect(dl_vols, 'out_files',
                                scan_volumes, 'volume_files')

        # Registration and modeling require a mask and bolus arrival.
        if mask:
            exec_wf.connect(input_spec, 'subject', mask, 'subject')
            exec_wf.connect(input_spec, 'session', mask, 'session')
            exec_wf.connect(input_spec, 'scan', mask, 'scan')
            if hasattr(mask.inputs, 'time_series'):
                exec_wf.connect(scan_ts, 'time_series',
                                mask, 'time_series')
                self.logger.debug('Connected the scan time series to mask.')

            # Registration requires a fixed reference volume index to
            # register against, determined as follows:
            # * If the registration reference option is set, then that
            #   is used.
            # * Otherwise, if there is a ROI workflow, then the ROI
            #   volume serves as the fixed volume.
            # * Otherwise, the computed bolus arrival is the fixed
            #   volume.
            compute_reg_reference = (
                register and not roi
                and not register.inputs.reference
            )
            is_bolus_arrival_required = (
                compute_reg_reference or
                (model and self.modeling_technique != 'Mock')
            )
            # Modeling always requires the bolus arrival.
            bolus_arrival = None
            if is_bolus_arrival_required:
                # Compute the bolus arrival from the scan time series.
                bolus_arv_xfc = Function(input_names=['time_series'],
                                         output_names=['volume'],
                                         function=_bolus_arrival)
                bolus_arrival = pe.Node(bolus_arv_xfc, name='bolus_arrival')
                exec_wf.connect(scan_ts, 'time_series',
                                bolus_arrival, 'time_series')
                self.logger.debug('Connected the scan time series to the bolus'
                                  ' arrival calculation.')

        # If ROI is enabled, then convert the ROIs using the scan
        # time series.
        if roi:
            exec_wf.connect(input_spec, 'subject', roi, 'subject')
            exec_wf.connect(input_spec, 'session', roi, 'session')
            exec_wf.connect(input_spec, 'scan', roi, 'scan')
            exec_wf.connect(scan_ts, 'time_series', roi, 'time_series')
            self.logger.debug('Connected the scan time series to ROI.')

        # If registration is enabled, then register the staged images.
        if register:
            exec_wf.connect(input_spec, 'subject', register, 'subject')
            exec_wf.connect(input_spec, 'session', register, 'session')
            exec_wf.connect(input_spec, 'scan', register, 'scan')

            # The registration input files are either staged or downloaded.
            exec_wf.connect(scan_volumes, 'volume_files',
                            register, 'in_files')
            # The mask input.
            if mask:
                exec_wf.connect(mask, 'out_file', register, 'mask')
                self.logger.debug('Connected the mask to registration.')

            # If the ROI workflow is enabled, then register against
            # the ROI volume. Otherwise, use the bolus arrival volume.
            if not register.inputs.reference:
                if roi:
                    # Get the ROI volume number from any ROI file.
                    roi_vol = self.roi_files[0].volume
                    register.inputs.reference = roi_vol
                    self.logger.debug(
                        "Set the registration reference to the ROI"
                        " volume %d." % roi_vol
                    )
                elif bolus_arrival:
                    exec_wf.connect(bolus_arrival, 'volume',
                                    register, 'reference')
                    self.logger.debug('Connected bolus arrival to the'
                                      ' registration reference.')

        # If the modeling workflow is enabled, then model the scan or
        # realigned images.
        if model:
            exec_wf.connect(input_spec, 'subject', model, 'subject')
            exec_wf.connect(input_spec, 'session', model, 'session')
            exec_wf.connect(input_spec, 'scan', model, 'scan')
            # The mask input.
            if mask:
                exec_wf.connect(mask, 'out_file', model, 'mask')
                self.logger.debug('Connected the mask to modeling.')
            # The bolus arrival input.
            if bolus_arrival:
                exec_wf.connect(bolus_arrival, 'volume',
                                model, 'bolus_arrival')
            self.logger.debug('Connected bolus arrival to modeling.')

            # Obtain the modeling input 4D time series.
            if is_scan_modeling:
                # There is no register action and no registration
                # resource option. In that case, model the scan
                # input. scan_ts is always created previously if
                # is_scan_modeling is true.
                exec_wf.connect(scan_ts, 'time_series', model, 'time_series')
            elif register:
                exec_wf.connect(register, 'time_series', model, 'time_series')
                self.logger.debug('Connected registration to modeling.')
            else:
                # Validate the XNAT registration time series. Note
                # that self.registration_resource is set since
                # is_scan_modeling is false and register is None.
                reg_ts_name = self.registration_resource + '_ts.nii.gz'
                with qixnat.connect() as xnat:
                    has_reg_ts = _scan_file_exists(
                        xnat, self.project, scan_input,
                        self.registration_resource, reg_ts_name
                    )
                # The time series must have been created by the
                # registration process.
                if not has_reg_ts:
                    raise PipelineError(
                        "The %s %s scan %d registration resource %s does"
                        " not include the time series file %s" %
                        (scan_input.subject, scan_input.session,
                         scan_input.scan, self.registration_resource,
                         reg_ts_name)
                    )
                # Download the registration time series.
                dl_reg_ts_xfc = XNATDownload(
                    project=self.project,
                    resource=self.registration_resource,
                    file=reg_ts_name
                )
                dl_reg_ts = pe.Node(dl_reg_ts_xfc,
                                    name='download_reg_time_series')
                exec_wf.connect(input_spec, 'subject', dl_reg_ts, 'subject')
                exec_wf.connect(input_spec, 'session', dl_reg_ts, 'session')
                exec_wf.connect(input_spec, 'scan', dl_reg_ts, 'scan')
                # Pass the realigned time series to modeling.
                exec_wf.connect(dl_reg_ts, 'out_file', model, 'time_series')

        # Set the configured workflow node inputs and plug-in options.
        self._configure_nodes(exec_wf)

        self.logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(exec_wf)

        return exec_wf

    def _run_workflow(self):
        """
        Overrides the superclass method to build the child workflows
        if the *dry_run* instance variable flag is set.
        """
        super(QIPipelineWorkflow, self)._run_workflow()
        if self.dry_run:
            # Make a dummy temp directory and files for simulating
            # the called workflows. These workflows inherit the
            # dry_run flag from this parent workflow and only go
            # through the motions of execution.
            dummy_dir = tempfile.mkdtemp()
            dummy_volume = "%s/volume001.nii.gz" % dummy_dir
            open(dummy_volume, 'a').close()
            _, dummy_roi = tempfile.mkstemp(dir=dummy_dir, prefix='roi')
            _, dummy_mask = tempfile.mkstemp(dir=dummy_dir, prefix='mask')
            _, dummy_ts = tempfile.mkstemp(dir=dummy_dir, prefix='ts')
            opts = self._child_options()
            try:
                # If staging is enabled, then simulate it.
                if self.workflow.get_node('stage'):
                    input_spec = self.workflow.get_node('input_spec')
                    in_dirs = input_spec.inputs.in_dirs
                    stg_opts = dict(collection=self.collection.name,
                                    dest=dummy_dir, **opts)
                    _stage('Breast001', 'Session01', 1, in_dirs, stg_opts)
                # If registration is enabled, then simulate it.
                if self.workflow.get_node('register'):
                    _register('Breast001', 'Session01', 1, [dummy_volume],
                              opts)
                # If ROI is enabled, then simulate it.
                if self.workflow.get_node('roi'):
                    # A dummy (lesion, slice index, in_file) ROI input tuple.
                    inputs = [LesionROI(1, 1, 1, dummy_roi)]
                    _roi('Breast001', 'Session01', 1, dummy_ts, inputs, opts)
                # If modeling is enabled, then simulate it.
                if self.workflow.get_node('model'):
                    mdl_opts = dict(technique=self.modeling_technique,
                                    bolus_arrival=1, **opts)
                    _model('Breast001', 'Session01', 1, dummy_ts, mdl_opts)
            finally:
                shutil.rmtree(dummy_dir)


def _collect_roi_files(collection, scan_input):
    """
    :return: the ROI files
    """
    _logger = logger(__name__)
    roi_dirs = scan_input.roi
    if roi_dirs:
        img_coll = image_collection.with_name(collection)
        scan_pats = img_coll.patterns.scan[scan_input.scan]
        if not scan_pats:
            raise PipelineError("Scan patterns were not found"
                                " for %s %s scan %d" % (
                                scan_input.subject, scan_input.session,
                                scan_input.scan))
        glob = scan_pats.roi.glob
        regex = scan_pats.roi.regex
        _logger.debug(
            "Discovering %s %s scan %d ROI files matching %s..." %
            (scan_input.subject, scan_input.session, scan_input.scan,
             glob)
        )
        roi_inputs = list(iter_roi(regex, *roi_dirs))
        if roi_inputs:
            _logger.info(
                "%d %s %s scan %d ROI files were discovered." %
                (len(roi_inputs), scan_input.subject,
                 scan_input.session, scan_input.scan)
            )
        else:
            _logger.info(
                "No ROI file was detected for %s %s scan %d." %
                (scan_input.subject, scan_input.session, scan_input.scan)
            )
    else:
        _logger.info(
            "ROI directory was not detected for %s %s scan %d." %
            (scan_input.subject, scan_input.session, scan_input.scan)
        )
        roi_inputs = []

    return roi_inputs


def exclude_files(in_files, exclusions):
    """
    :param in_files: the input file paths
    :param exclusions: the file names to exclude
    :return: the filtered input file paths
    """
    import os

    # Make the exclusions a set.
    exclusions = set(exclusions)

    # Filter the input files.
    return [f for f in in_files
            if os.path.split(f)[1] not in exclusions]


def _bolus_arrival(time_series):
    """
    Determines the bolus uptake volume number. If it could not
    be determined, then the first time point is taken to be the
    uptake volume.

    :param time_series: the 4D time series image
    :return: the bolus arrival volume number, or 1 if the arrival
        cannot be calculated
    """
    from qipipe.helpers.bolus_arrival import (bolus_arrival_index,
                                              BolusArrivalError)

    try:
        return bolus_arrival_index(time_series) + 1
    except BolusArrivalError:
        return 1


def _stage(subject, session, scan, in_dirs, opts):
    """
    Runs the staging workflow on the given session scan images.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_dirs: the input DICOM directories
    :param opts: the :meth:`qipipe.pipeline.staging.run` keyword options
    :return: the :meth:`qipipe.staging.run` result
    """
    from qipipe.pipeline import staging

    return staging.run(subject, session, scan, *in_dirs, **opts)


def _roi(subject, session, scan, time_series, in_rois, opts):
    """
    Runs the ROI workflow on the given session scan images.

    .. Note:: see the :meth:`register` note.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param time_series: the scan 4D time series
    :param in_rois: the :meth:`qipipe.pipeline.roi.run` input ROI specs
    :param opts: the :meth:`qipipe.pipeline.roi.run` keyword options
    :return: the ROI mask file
    """
    from qipipe.pipeline import roi
    from qipipe.helpers.logging import logger

    return roi.run(subject, session, scan, time_series, *in_rois, **opts)


def _register(subject, session, scan, in_files, opts, reference=1,
              mask=None):
    """
    A facade for the :meth:`qipipe.pipeline.registration.register
    method.

    .. Note:: The *reference* and *mask* parameters are
      registration options, but can't be included in the *opts*
      parameter, since they are potential upstream workflow node
      connection points. Since a mock registration technique does
      not connect these inputs, theyt have a default value in the
      method signature as well.

    .. Note:: contrary to Python convention, the *opts* method
      parameter is a required dictionary rather than a keyword
      double-splat argument (i.e., ``**opts``). The Nipype
      ``Function`` interface does not support double-splat
      arguments.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_files: the input session scan 3D NIfTI images
    :param opts: the :meth:`qipipe.pipeline.registration.run`
        keyword options
    :param reference: the reference volume number
    :param mask: the mask file, required unless the model
        technique is ``Mock``
    :return: the :meth:`qipipe.pipeline.registration.run`
        output time series
    """
    from qipipe.pipeline import registration
    from nipype.interfaces.traits_extension import isdefined

    # Transform a Nipype undefined to the default value.
    if not isdefined(mask):
        mask = None

    return registration.run(
        subject, session, scan, in_files, reference=reference,
        mask=mask, **opts
    )


def _mask(subject, session, scan, time_series, opts):
    """
    Runs the mask workflow on the given session scan time series.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param time_series: the scan 4D time series
    :param opts: the :meth:`qipipe.pipeline.mask.run` keyword options
    :return: the mask file absolute path
    """
    from qipipe.pipeline import mask

    return mask.run(subject, session, scan, time_series, **opts)


def _model(subject, session, scan, time_series, opts,
           bolus_arrival=1, mask=None):
    """
    Runs the modeling workflow on the given time series.
    *mask* and *bolus_arrival_index* are
    :class:`qipipe.pipeline.modeling.ModelingWorkflow` options,
    but are required input to this ``model`` function.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param time_series: the scan or registration 4D time series
    :param opts: the :meth:`qipipe.pipeline.modeling.run` keyword options
    :param bolus_arrival: the required bolus arrival volume number
    :param mask: the mask file, required unless the model technique
        is ``Mock``
    :return: the modeling result dictionary
    """
    from qipipe.pipeline import modeling

    bolus_arrival_index = bolus_arrival - 1
    return modeling.run(
        subject, session, scan, time_series, mask=mask,
        bolus_arrival_index=bolus_arrival_index, **opts
    )
