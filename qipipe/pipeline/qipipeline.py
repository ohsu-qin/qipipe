import os
import re
import shutil
import tempfile
import logging
# The ReadTheDocs build does not include nipype.
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
if not on_rtd:
    # Disable nipype nipy import FutureWarnings.
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        from nipype.pipeline import engine as pe
        from nipype.interfaces.utility import (IdentityInterface, Function, Merge)
        from nipype.interfaces.dcmstack import MergeNifti
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
from ..staging.roi import LesionROI
from .modeling import (ModelingWorkflow, MODELING_CONF_FILE)
from .mask import MaskWorkflow
from ..helpers.constants import (SCAN_TS_RSC, MASK_RSC)
from ..interfaces import (XNATDownload, XNATUpload)

SINGLE_VOLUME_ACTIONS = ['stage', 'roi']
"""The workflow actions which apply to a single-volume scan."""

MULTI_VOLUME_ACTIONS = SINGLE_VOLUME_ACTIONS + ['qiprofile', 'register', 'model']
"""The workflow actions which apply to a multi-volume scan."""

VOLUME_FILE_PAT = re.compile("volume(\d{3}).nii.gz$")
"""
The volume file name pattern. The image file name is
volume<number>.nii.gz, where <number> is the zero-padded volume
number, as determined by the
:meth:`qipipe.pipeline.staging.volume_format` function.
"""


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
            raise ValueError("The ROI pipeline can only be run"
                             " with staging or stand-alone")
        _run_with_dicom_input(actions, *inputs, **opts)
    else:
        # Run downstream actions with XNAT session input.
        _run_with_xnat_input(actions, *inputs, **opts)


def _run_with_dicom_input(actions, *inputs, **opts):
    """
    :param actions: the actions to perform
    :param inputs: the DICOM directories to process
    :param opts: the :meth:`run` options
    """
    # The required image collection name.
    collection = opts.pop('collection', None)
    if not collection:
        raise ArgumentError('The staging pipeline collection option'
                            ' is missing.')
    # The required XNAT project name.
    project = opts.pop('project', None)
    if not project:
        raise ArgumentError('The staging pipeline project option'
                            ' is missing.')

    # The set of input subjects is used to build the CTP mapping file
    # after the workflow is completed, if staging is enabled.
    subjects = set()
    # Run the workflow on each session and scan.
    for scan_input in iter_stage(project, collection, *inputs, **opts):
        wf_actions = _filter_actions(scan_input, actions)
        # Capture the subject.
        subjects.add(scan_input.subject)
        # If only ROI is enabled, then check for an existing scan
        # time series.
        if set(wf_actions) == {'roi'}:
            with qixnat.connect() as xnat:
                wf_opts = opts.copy()
                if _scan_resource_exists(xnat, project, scan_input.subject,
                                         scan_input.session, scan_input.scan,
                                         SCAN_TS_RSC):
                    wf_opts['scan_time_series'] = SCAN_TS_RSC
        else:
            wf_opts = opts
        # Create a new workflow.
        wf_gen = QIPipelineWorkflow(project, wf_actions, collection=collection,
                                    **wf_opts)
        # Run the workflow on the scan.
        wf_gen.run_with_dicom_input(wf_actions, collection, scan_input)

    # If staging is enabled, then make the TCIA subject map.
    if 'stage' in actions:
        map_ctp(collection, *subjects, dest=wf_gen.dest)


def _filter_actions(scan_input, actions):
    """
    Filters the specified actions for the given scan input.
    If the scan number is in the :const:`MULTI_VOLUME_SCAN_NUMBERS`,
    then this method returns the specified actions. Otherwise,
    this method returns the actions allowed as
    :const:`SINGLE_VOLUME_ACTIONS`.

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
            "Skipping the %s %s %s scan %d, since the scan is a single-volume"
            " scan and only the actions %s are supported for a single-volume"
            " scan." %
            (scan_input.collection, scan_input.subject, scan_input.session,
             scan_input.scan, actions, SINGLE_VOLUME_ACTIONS)
        )
    elif disallowed:
        logger(__name__).debug(
            "Ignoring the %s %s %s scan %d actions %s, since the scan"
            " is a single-volume scan and only the actions %s are"
            " supported for a single-volume scan." %
            (scan_input.collection, scan_input.subject, scan_input.session,
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
            raise ArgumentError("The XNAT path is missing a project: %s" % path)
        sbj = hierarchy.pop('subject', None)
        if not sbj:
            raise ArgumentError("The XNAT path is missing a subject: %s" % path)
        sess = hierarchy.pop('experiment', None)
        if not sess:
            raise ArgumentError("The XNAT path is missing a session: %s" % path)
        scan_s = hierarchy.pop('scan', None)
        if not scan_s:
            raise ArgumentError("The XNAT path is missing a scan: %s" % path)
        scan = int(scan_s)
        # The XNAT connection is open while the input scan is processed.
        with qixnat.connect() as xnat:
            # The XNAT scan object must exist.
            scan_obj = xnat.find_one(prj, sbj, sess, scan=scan)
            if not scan_obj:
                raise ArgumentError("The XNAT scan object does not exist: %s" %
                                    path)

            # The workflow options are augmented from the base options.
            wf_opts = dict(opts)
            # Check for an existing mask.
            if _scan_resource_exists(xnat, prj, sbj, sess, scan, MASK_RSC):
                wf_opts['mask'] = MASK_RSC

            # Every post-stage action requires a 4D scan time series.
            ts_actions = (action for action in actions if action != 'stage')
            if any(ts_actions):
                if _scan_resource_exists(xnat, prj, sbj, sess, scan,
                                         SCAN_TS_RSC):
                    wf_opts['scan_time_series'] = SCAN_TS_RSC

            # If modeling will be performed on a specified registration
            # resource, then check for an existing 4D registration time
            # series.
            reg_rsc_opt = opts.get('registration_resource')
            if 'model' in actions and reg_rsc_opt:
                reg_ts_name = reg_rsc_opt + '_ts.nii.gz'
                file_obj = xnat.find_one(prj, sbj, sess, scan=scan,
                                         resource=reg_rsc_opt,
                                         file=reg_ts_name)
                if file_obj:
                    wf_opts['realigned_time_series'] = reg_ts_name

        # Make the workflow.
        wf_gen = QIPipelineWorkflow(prj, actions, **wf_opts)
        # If a registration resource was specified, then set the flag
        # to check for registered scans.
        check_reg = opts.get('registration_resource') != None
        # Run the workflow.
        wf_gen.run_with_scan_download(prj, sbj, sess, scan, actions,
                                      check_reg)


def _scan_resource_exists(xnat, project, subject, session, scan, resource):
    """
    :return: whether the given XNAT scan resource exists
    """
    rsc_obj = xnat.find_one(project, subject, session, scan=scan,
                            resource=resource)
    exists = rsc_obj and rsc_obj.files().get()
    status = 'found' if exists else 'not found'
    logger(__name__).debug("The %s %s %s scan %d resource %s was %s." %
                           (project, subject, session, scan, resource,
                            status))

    return exists


class ArgumentError(Exception):
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

    - If staging is performed, then the DICOM files are staged for the
      subject directory inputs. Otherwise, staging is not performed.
      In that case, if registration is enabled as described below, then
      the previously staged volume scan stack images are downloaded.

    - If registration is performed and the ``registration`` resource
      option is set, then the previously realigned images with the
      given resource name are downloaded. The remaining volumes are
      registered.

    - If registration or modeling is performed and the XNAT ``mask``
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

    def __init__(self, project, actions, **opts):
        """
        :param project: the XNAT project name
        :param actions: the actions to perform
        :param opts: the :class:`qipipe.staging.WorkflowBase`
            initialization options as well as the following keyword arguments:
        :keyword dest: the staging destination directory
        :keyword mask: the XNAT mask resource name
        :keyword collection: the image collection name
        :keyword registration_resource: the XNAT registration resource
            name
        :keyword registration_technique: the
            class:`qipipe.pipeline.registration.RegistrationWorkflow`
            technique
        :keyword recursive_registration: the
            class:`qipipe.pipeline.registration.RegistrationWorkflow`
            recursive flag
        :keyword modeling_resource: the modeling resource name
        :keyword modeling_technique: the
            class:`qipipe.pipeline.modeling.ModelingWorkflow` technique
        :keyword scan_time_series: the scan time series resource name
        :keyword realigned_time_series: the registered time series resource
            name
        """
        super(QIPipelineWorkflow, self).__init__(
            project=project, name=__name__, **opts
        )

        collOpt = opts.get('collection')
        if collOpt:
            self.collection = image_collection.with_name(collOpt)
        else:
            self.collection = None

        reg_tech_opt = opts.get('registration_technique')
        if reg_tech_opt:
            self.registration_technique = reg_tech_opt.lower()
            """The registration technique."""
        elif 'register' in actions:
            raise PipelineError('The registration technique was not specified.')

        reg_rsc_opt = opts.get('registration_resource')
        if reg_rsc_opt:
            reg_rsc = reg_rsc_opt
        elif 'register' in actions:
            reg_rsc = registration.generate_resource_name()
            self.logger.debug("Generated registration resource name %s" %
                               reg_rsc)
        else:
            reg_rsc = None
        self.registration_resource = reg_rsc
        """The registration XNAT resource name."""

        mdl_tech_opt = opts.get('modeling_technique')
        if mdl_tech_opt:
            self.modeling_technique = mdl_tech_opt.lower()
            """The modeling technique name."""
        elif 'model' in actions:
            raise PipelineError('The modeling technique was not specified.')

        mdl_rsc_opt = opts.get('modeling_resource')
        if mdl_rsc_opt:
            mdl_rsc = mdl_rsc_opt
        elif 'model' in actions:
            mdl_rsc = modeling.generate_resource_name()
        else:
            mdl_rsc = None
        self.modeling_resource = mdl_rsc
        """The modeling XNAT resource name."""

        self.workflow = self._create_workflow(actions, **opts)
        """
        The pipeline execution workflow. The execution workflow is executed
        by calling the :meth:`run_with_dicom_input` or
        :meth:`run_with_scan_download` method.
        """

    def run_with_dicom_input(self, actions, collection, scan_input):
        """
        :param actions: the workflow actions to perform
        :param scan_input: the {subject, session, scan, dicom, roi}
            object
        :param dest: the TCIA staging destination directory (default is
            the current working directory)
        """
        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.collection = collection
        input_spec.inputs.subject = scan_input.subject
        input_spec.inputs.session = scan_input.session
        input_spec.inputs.scan = scan_input.scan
        input_spec.inputs.in_dirs = scan_input.dicom
        input_spec.inputs.registered = []

        # If roi is enabled and has input, then set the roi function inputs.
        if 'roi' in actions:
            roi_dirs = scan_input.roi
            if roi_dirs:
                roi_files = []
                if not self.collection:
                    raise PipelineError("ROI workflow requires the"
                                        " collection option")
                scan_pats = self.collection.patterns.scan[scan_input.scan]
                if not scan_pats:
                    raise PipelineError("Scan patterns were not found"
                                        " for %s %s scan %d" % (
                                        scan_input.subject, scan_input.session,
                                        scan_input.scan))
                regex = scan_pats.roi.regex
                for d in roi_dirs:
                    candidates = ('/'.join([d, f]) for f in os.listdir(d))
                    if regex:
                        files = (f for f in candidates if regex.match(f))
                    else:
                        files = candidates
                    roi_files.extend(files)
                if roi_files:
                    self.logger.info(
                        "%d %s %s scan %d ROI files were discovered." %
                        (len(roi_files), scan_input.subject,
                         scan_input.session, scan_input.scan)
                    )
                    self._set_roi_inputs(*roi_files)
                else:
                    self.logger.info("No ROI file was detected for"
                                      " %s %s scan %d." %
                                      (scan_input.subject, scan_input.session,
                                       scan_input.scan))
            else:
                self.logger.info("ROI directory was not detected for"
                                  " %s %s scan %d" % (scan_input.subject,
                                  scan_input.session, scan_input.scan))
        # Execute the workflow.
        self.logger.info("Running the pipeline on %s %s scan %d." %
                           (scan_input.subject, scan_input.session,
                            scan_input.scan))
        self._run_workflow(self.workflow)
        self.logger.info("Completed pipeline execution on %s %s scan %d." %
                           (scan_input.subject, scan_input.session,
                            scan_input.scan))

    def run_with_scan_download(self, project, subject, session, scan, actions,
                               is_existing_registration_resource):
        """
        Runs the execution workflow on downloaded scan image files.

        :param project: the project name
        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param actions: the workflow actions
        :param is_existing_registration_resource: flag indicating
            whether an existing registration resource name was
            specfied
        """
        self.logger.debug("Processing the %s %s %s Scan %d volumes..." %
                           (project, subject, session, scan))
        # Get the volume numbers.
        with qixnat.connect() as xnat:
            scan_obj = xnat.find_one(project, subject, session, scan=scan)
            if not scan_obj:
                raise NotFoundError("The pipeline did not find a %s %s %s"
                                    " scan %d." %
                                    (project, subject, session, scan))
            # The NIFTI resource contains the volume files.
            rsc_obj = scan_obj.resource('NIFTI')
            if not rsc_obj.exists():
                raise NotFoundError("The pipeline did not find a %s %s %s"
                                    " scan %d NIFTI resource." %
                                    (project, subject, session, scan))
            # The volume files.
            files = rsc_obj.files().get()
            if not files:
                raise ArgumentError("There are no pipeline %s %s %s Scan %d"
                                    " NIFTI volumes" %
                                    (project, subject, session, scan))

            # If the registration resource already exists in XNAT, then
            # partition the scan image file names into those which are
            # already registered and those which need to be registered.
            if is_existing_registration_resource:
                registered, unregistered = self._partition_registered(
                    xnat, project, subject, session, scan, files
                )
            else:
                registered = []
                unregistered = files

        # Validate and log the partition.
        if 'register' in actions:
            if registered:
                if unregistered:
                    self.logger.debug("Skipping registration of %d"
                                       " previously registered %s %s %s"
                                       " Scan %d volumes:" %
                                       (len(registered), project, subject,
                                        session, scan))
                    self.logger.debug("%s" % registered)
                else:
                    self.logger.debug("Skipping %s %s %s Scan %d"
                                       " registration, since all volumes"
                                       " are already registered." %
                                       (project, subject, session, scan))
            if unregistered:
                self.logger.debug("Registering %d %s %s %s Scan %d"
                                   " volumes:" %
                                   (len(unregistered), project, subject,
                                    session, scan))
                self.logger.debug("%s" % unregistered)
        elif unregistered and is_existing_registration_resource:
            self.logger.error("The pipeline %s %s %s Scan %d register"
                                " action is not specified but there are"
                                "  %d unregistered volumes:" %
                                (project, subject, session, scan,
                                 len(unregistered)))
            self.logger.error("%s" % unregistered)
            raise ArgumentError("The pipeline %s %s %s Scan %d register"
                                " action is not specified but there are"
                                " unregistered volumes" %
                                (project, subject, session, scan))
        else:
            self.logger.debug("Processing %d %s %s %s Scan %d volumes:" %
                               (len(unregistered), project, subject, session,
                                scan))
            self.logger.debug("%s" % unregistered)

        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.registered = registered

        # Execute the workflow.
        self._run_workflow(self.workflow)
        self.logger.debug("Registered %d %s %s %s Scan %d volumes:" %
                           (len(unregistered), project, subject, session,
                            scan))
        self.logger.debug("%s" % unregistered)

    def _set_roi_inputs(self, *inputs):
        """
        :param inputs: the :meth:`roi` inputs
        """
        # Set the roi function inputs.
        roi_node = self.workflow.get_node('roi')
        roi_node.inputs.in_rois = inputs

    def _partition_registered(self, xnat, project, subject, session, scan,
                              files):
        """
        Partitions the given volume file names into those which have a
        corresponding registration resource file and those which don't.

        :return: the (registered, unregistered) file names
        """
        # The XNAT registration object.
        if not self.registration_resource:
            raise ArgumentError("The registration resource is missing")
        reg_obj = xnat.find_one(project, subject, session, scan=scan,
                                resource=self.registration_resource)
        if not reg_obj:
            raise ArgumentError("The registration resource %s does not exist"
                                " in XNAT: %s" % self.registration_resource)

        # The realigned files.
        registered = set(reg_obj.files().get())
        # The unregistered volume numbers.
        unregistered = set(files) - registered
        self.logger.debug("The %s %s %s resource has %d registered volumes"
                           " and %d unregistered volumes." %
                           (project, subject, session, len(registered),
                            len(unregistered)))

        return registered, unregistered

    def _create_workflow(self, actions, **opts):
        """
        Builds the reusable pipeline workflow described in
        :class:`qipipe.pipeline.qipipeline.QIPipeline`.

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
        self.logger.debug("Building the pipeline execution workflow"
                            " for the actions %s..." % actions)
        # The execution workflow.
        exec_wf = pe.Workflow(name='qipipeline', base_dir=self.base_dir)

        # The resource names.
        reg_rsc_opt = opts.get('registration_resource')
        recursive_reg_opt = opts.get('recursive_registration')
        mdl_rsc_opt = opts.get('modeling_resource')
        mask_rsc_opt = opts.get('mask')
        scan_ts_rsc_opt = opts.get('scan_time_series')
        reg_ts_name_opt = opts.get('realigned_time_series')

        # The technique options.
        reg_tech_opt = opts.get('registration_technique')
        mdl_tech_opt = opts.get('modeling_technique')

        # The staging options.
        dest_opt = opts.get('dest')

        if 'model' in actions:
            mdl_wf_gen = ModelingWorkflow(parent=self, technique=mdl_tech_opt,
                                          resource=self.modeling_resource)
            mdl_wf = mdl_wf_gen.workflow
            self.modeling_resource = mdl_wf_gen.resource
        else:
            mdl_wf = None

        # The registration workflow node.
        if 'register' in actions:
            reg_inputs = ['subject', 'session','scan', 'reference_index',
                          'mask', 'in_files', 'opts']
            # The registration function keyword options.
            if not self.registration_technique:
                raise ArgumentError('Missing the registration technique')
            if not self.registration_resource:
                raise ArgumentError('Missing the registration resource name')
            # Spell out the registration workflow options rather
            # than delegating to this qipipeline workflow as the
            # parent, since Nipype Function arguments must be
            # primitive.
            reg_opts = self._child_options()
            reg_opts['resource'] = self.registration_resource
            reg_opts['technique'] = self.registration_technique
            if 'recursive_registration' in opts:
                reg_opts['recursive'] = opts['recursive_registration']
            # The registration function.
            reg_xfc = Function(input_names=reg_inputs,
                               output_names=['out_files'],
                               function=_register)
            reg_node = pe.Node(reg_xfc, name='register')
            reg_node.inputs.opts = reg_opts
            self.logger.info("Enabled registration with options %s." %
                             reg_opts)
        else:
            self.logger.info("Skipping registration.")
            reg_node = None

        # The ROI workflow node.
        if 'roi' in actions:
            roi_inputs = ['subject', 'session', 'scan', 'time_series',
                          'in_rois', 'opts']
            roi_xfc = Function(input_names=roi_inputs,
                               output_names=['volume'],
                               function=roi)
            roi_node = pe.Node(roi_xfc, name='roi')
            roi_opts = self._child_options()
            roi_node.inputs.opts = roi_opts
            self.logger.info("Enabled ROI conversion with options %s." %
                             roi_opts)
        else:
            roi_node = None
            self.logger.info("Skipping ROI conversion.")

        # Registration and modeling require a mask.
        if (reg_node or mdl_wf) and not mask_rsc_opt:
            if self.collection:
                crop_posterior = self.collection.crop_posterior
            else:
                crop_posterior = False
            mask_opts = self._child_options()
            mask_opts['crop_posterior'] = crop_posterior
            mask_wf_gen = MaskWorkflow(**mask_opts)
            mask_wf = mask_wf_gen.workflow
            self.logger.info("Enabled scan mask creation with options %s." %
                             mask_opts)
        else:
            mask_wf = None
            self.logger.info("Skipping scan mask creation.")

        # The staging workflow.
        if 'stage' in actions:
            stg_inputs = ['subject', 'session', 'scan', 'in_dirs', 'opts']
            stg_xfc = Function(input_names=stg_inputs,
                               output_names=['out_files'],
                               function=stage)
            stg_node = pe.Node(stg_xfc, name='stage')
            # The target directory.
            if dest_opt:
                self.dest = os.abspath(dest_opt)
            else:
                self.dest = os.getcwd()
            # It would be preferable to pass this QIPipelineWorkflow
            # in the *parent* option, but that induces the following
            # Nipype bug:
            # * A node input which includes a compiled regex
            #   results in the Nipype run error:
            #     TypeError: cannot deepcopy this pattern object
            # The work-around is to break out the separate
            # simple options that the WorkflowBase constructor
            # extracts from the parent.
            stg_opts = self._child_options()
            stg_opts['dest'] = self.dest
            stg_node.inputs.opts = stg_opts
            self.logger.info("Enabled staging with options %s" % stg_opts)
        else:
            stg_node = None
            self.logger.info("Skipping staging.")

        # Validate that there is at least one constituent workflow.
        if not any([stg_node, roi_node, reg_node, mdl_wf]):
            raise ArgumentError("No workflow was enabled.")

        # The workflow input fields.
        input_fields = ['subject', 'session', 'scan']
        # The staging workflow has additional input fields.
        # Partial registration requires the unregistered volumes input.
        if stg_node:
            input_fields.extend(['in_dirs'])
        elif reg_node:
            input_fields.append('registered')

        # The workflow input node.
        input_spec_xfc = IdentityInterface(fields=input_fields)
        input_spec = pe.Node(input_spec_xfc, name='input_spec')
        # Staging, registration, and mask require a volume iterator node.
        # Modeling requires a volume iterator node if and only if the
        # following conditions hold:
        # * modeling is performed on the scan time series, and
        # * the scan time series is not specified
        model_reg = reg_node or reg_ts_name_opt
        model_scan = not model_reg
        model_vol = model_scan and not scan_ts_rsc_opt

        # Stitch together the workflows:

        # If staging is enabled, then stage the DICOM input.
        if stg_node:
            for field in input_spec.inputs.copyable_trait_names():
                exec_wf.connect(input_spec, field, stg_node, field)

        # Some workflows require the scan volumes, as follows:
        # * If staging is enabled, then collect the staged NIfTI
        #   scan images.
        # * Otherwise, if registration is enabled and there is not
        #   yet a scan time series, then download the staged XNAT
        #   scan images.
        # In either of the above cases, the staged images are collected
        # in a node named 'staged' with output 'images', which is used
        # later in the pipeline.
        # Otherwise, there is no staged node.
        staged = None
        if reg_node or roi_node or mask_wf or (mdl_wf and not model_reg):
            if stg_node:
                staged = stg_node
            elif reg_node or not scan_ts_rsc_opt:
                dl_scan_xfc = XNATDownload(project=self.project,
                                           resource='NIFTI')
                staged = pe.Node(dl_scan_xfc, name='staged')
                exec_wf.connect(input_spec, 'subject', staged, 'subject')
                exec_wf.connect(input_spec, 'session', staged, 'session')
                exec_wf.connect(input_spec, 'scan', staged, 'scan')

        # All downstream actions require a scan time series.
        if reg_node or mask_wf or roi_node or mdl_wf:
            # If there is a scan time series, then download it.
            # Otherwise, if staging is enabled, then stack the resulting
            # staged 3D images into the scan time series.
            # Any other case is an error.
            if scan_ts_rsc_opt:
                dl_scan_ts_xfc = XNATDownload(project=self.project,
                                              resource=SCAN_TS_RSC)
                scan_ts = pe.Node(dl_scan_ts_xfc,
                                  name='download_scan_time_series')
                exec_wf.connect(input_spec, 'subject', scan_ts, 'subject')
                exec_wf.connect(input_spec, 'session', scan_ts, 'session')
                exec_wf.connect(input_spec, 'scan', scan_ts, 'scan')
            elif staged:
                # Merge the staged files.
                collection = opts.get('collection')
                if not collection:
                    raise ArgumentError('The scan time series pipeline'
                                        ' collection option is missing.')
                # The volume grouping tag.
                if self.collection:
                    vol_tag = self.collection.patterns.volume
                else:
                    vol_tag = None
                if not vol_tag:
                    raise ArgumentError('The scan time series pipeline'
                                        ' collection volume tag is missing.')
                scan_ts_xfc = MergeNifti(sort_order=[vol_tag],
                                         out_format=SCAN_TS_RSC)
                scan_ts = pe.Node(scan_ts_xfc, name='merge_volumes')
                exec_wf.connect(staged, 'out_files', scan_ts, 'in_files')
                self.logger.debug('Connected staging to scan time series merge.')
                # Upload the time series.
                ul_scan_ts_xfc = XNATUpload(project=self.project,
                                            resource=SCAN_TS_RSC,
                                            modality='MR')
                ul_scan_ts = pe.Node(ul_scan_ts_xfc,
                                     name='upload_scan_time_series')
                exec_wf.connect(input_spec, 'subject', ul_scan_ts, 'subject')
                exec_wf.connect(input_spec, 'session', ul_scan_ts, 'session')
                exec_wf.connect(input_spec, 'scan', ul_scan_ts, 'scan')
                exec_wf.connect(scan_ts, 'out_file', ul_scan_ts, 'in_files')
            else:
                raise NotFoundError('The workflow requires a scan time series')

        # Registration and modeling require a mask and bolus arrival.
        if reg_node or mdl_wf:
            # If a mask resource name was specified, then download the mask.
            # Otherwise, make the mask.
            if mask_rsc_opt:
                dl_mask_xfc = XNATDownload(project=self.project,
                                           resource=mask_rsc_opt)
                download_mask = pe.Node(dl_mask_xfc, name='download_mask')
                exec_wf.connect(input_spec, 'subject', download_mask, 'subject')
                exec_wf.connect(input_spec, 'session', download_mask, 'session')
                exec_wf.connect(input_spec, 'scan', download_mask, 'scan')
            else:
                assert mask_wf, "The mask workflow is missing"
                exec_wf.connect(input_spec, 'subject',
                                mask_wf, 'input_spec.subject')
                exec_wf.connect(input_spec, 'session',
                                mask_wf, 'input_spec.session')
                exec_wf.connect(input_spec, 'scan',
                                mask_wf, 'input_spec.scan')
                exec_wf.connect(scan_ts, 'out_file',
                                mask_wf, 'input_spec.time_series')
                self.logger.debug('Connected scan time series to mask.')

            # Registration requires a fixed volume index to register against.
            # If there is a ROI workflow, then the ROI volume serves as
            # the fixed volume. Otherwise, the computed bolus arrival is
            # the fixed volume. Modeling always requires the bolus arrival.
            if mdl_wf or not roi_node:
                # Compute the bolus arrival from the scan time series.
                bolus_arv_xfc = Function(input_names=['time_series'],
                                         output_names=['bolus_arrival_index'],
                                         function=bolus_arrival_index_or_zero)
                bolus_arv = pe.Node(bolus_arv_xfc, name='bolus_arrival_index')
                exec_wf.connect(scan_ts, 'out_file', bolus_arv, 'time_series')
                self.logger.debug('Connected the scan time series to the bolus'
                                  ' arrival calculation.')

        # If ROI is enabled, then convert the ROIs.
        if roi_node:
            exec_wf.connect(input_spec, 'subject', roi_node, 'subject')
            exec_wf.connect(input_spec, 'session', roi_node, 'session')
            exec_wf.connect(input_spec, 'scan', roi_node, 'scan')
            exec_wf.connect(scan_ts, 'out_file', roi_node, 'time_series')
            self.logger.debug('Connected the scan time series to ROI.')

        # If registration is enabled, then register the unregistered
        # staged images.
        if reg_node:
            # There must be staged files.
            if not staged:
                raise NotFoundError('Registration requires a scan input')
            exec_wf.connect(input_spec, 'subject', reg_node, 'subject')
            exec_wf.connect(input_spec, 'session', reg_node, 'session')
            exec_wf.connect(input_spec, 'scan', reg_node, 'scan')

            # If the registration input files were downloaded from
            # XNAT, then select only the unregistered files.
            if stg_node:
                exec_wf.connect(staged, 'out_files', reg_node, 'in_files')
            else:
                exc_regd_xfc = Function(input_names=['in_files', 'exclusions'],
                                        output_names=['out_files'],
                                        function=exclude_files)
                exclude_registered = pe.Node(exc_regd_xfc,
                                             name='exclude_registered')
                exec_wf.connect(staged, 'out_files',
                                exclude_registered, 'in_files')
                exec_wf.connect(input_spec, 'registered',
                                exclude_registered, 'exclusions')
                exec_wf.connect(exclude_registered, 'out_files',
                                reg_node, 'in_files')
            self.logger.debug('Connected staging to registration.')

            # The mask input.
            if mask_wf:
                exec_wf.connect(mask_wf, 'output_spec.mask', reg_node, 'mask')
                self.logger.debug('Connected mask to registration.')
            else:
                exec_wf.connect(download_mask, 'out_file', reg_node, 'mask')

            # If the ROI workflow is enabled, then register against
            # the ROI volume. Otherwise, use the bolus arrival volume.
            if roi_node:
                exec_wf.connect(roi_node, 'volume',
                                reg_node, 'reference_index')
                self.logger.debug('Connected ROI volume to registration'
                                  'bolus arrival.')
            else:
                exec_wf.connect(bolus_arv, 'bolus_arrival_index',
                                reg_node, 'reference_index')
                self.logger.debug('Connected bolus arrival to registration.')

        # If the modeling workflow is enabled, then model the scan or
        # realigned images.
        if mdl_wf:
            exec_wf.connect(input_spec, 'subject',
                            mdl_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session',
                            mdl_wf, 'input_spec.session')
            exec_wf.connect(input_spec, 'scan',
                            mdl_wf, 'input_spec.scan')
            # The mask input.
            if mask_wf:
                exec_wf.connect(mask_wf, 'output_spec.mask',
                                mdl_wf, 'input_spec.mask')
                self.logger.debug('Connected mask to modeling.')
            else:
                exec_wf.connect(download_mask, 'out_file',
                                mdl_wf, 'input_spec.mask')
            # The bolus arrival.
            exec_wf.connect(bolus_arv, 'bolus_arrival_index',
                            mdl_wf, 'input_spec.bolus_arrival_index')
            self.logger.debug('Connected bolus arrival to modeling.')

            # Obtain the modeling input 4D time series.
            mdl_input_spec = mdl_wf.get_node('input_spec')
            if reg_ts_name_opt:
                # Download the XNAT time series file.
                ts_dl_xfc = XNATDownload(project=self.project,
                                         resource=self.registration_resource,
                                         file=reg_ts_name_opt)
                reg_ts = pe.Node(ts_dl_xfc, name='download_reg_time_series')
                exec_wf.connect(input_spec, 'subject', reg_ts, 'subject')
                exec_wf.connect(input_spec, 'session', reg_ts, 'session')
                exec_wf.connect(input_spec, 'scan', reg_ts, 'scan')
                mdl_input_spec.inputs.resource = self.registration_resource
                exec_wf.connect(reg_ts, 'out_file',
                                mdl_wf, 'input_spec.time_series')
            elif self.registration_resource:
                # Merge the realigned images to 4D.
                reg_ts_name = self.registration_resource + '_ts'
                merge_reg = pe.Node(MergeNifti(out_format=reg_ts_name),
                                    name='merge_reg')

                # If the registration resource name was specified,
                # then download the previously realigned images.
                if reg_rsc_opt:
                    reg_dl_xfc = XNATDownload(project=self.project,
                                              resource=reg_rsc_opt)
                    download_reg = pe.Node(reg_dl_xfc,
                                           name='download_realigned_images')
                    exec_wf.connect(input_spec, 'subject',
                                    download_reg, 'subject')
                    exec_wf.connect(input_spec, 'session',
                                    download_reg, 'session')
                    exec_wf.connect(input_spec, 'scan',
                                    download_reg, 'scan')
                    if reg_node:
                        # Merge the previously and newly realigned images.
                        concat_reg = pe.Node(Merge(2),
                                             name='concat_reg')
                        exec_wf.connect(download_reg, 'out_files',
                                        concat_reg, 'in1')
                        exec_wf.connect(reg_node, 'out_files',
                                        concat_reg, 'in2')
                        exec_wf.connect(concat_reg, 'out',
                                        merge_reg, 'in_files')
                    else:
                        # All of the realigned files were downloaded.
                        exec_wf.connect(download_reg, 'out_files',
                                        merge_reg, 'in_files')
                elif reg_node:
                    # All of the realigned files were created by the
                    # registration workflow.
                    exec_wf.connect(reg_node, 'out_files',
                                    merge_reg, 'in_files')
                else:
                    raise ArgumentError(
                        'The pipeline cannot perform modeling on the'
                        ' registration result, since the registration'
                        ' workflow is disabled and no registration resource'
                        ' was specified.')

                # Upload the realigned time series to XNAT.
                upload_reg_ts_xfc = XNATUpload(
                    project=self.project, resource=self.registration_resource,
                    modality='MR'
                )
                upload_reg_ts = pe.Node(upload_reg_ts_xfc,
                                        name='upload_reg_time_series')
                exec_wf.connect(input_spec, 'subject',
                                upload_reg_ts, 'subject')
                exec_wf.connect(input_spec, 'session',
                                upload_reg_ts, 'session')
                exec_wf.connect(input_spec, 'scan',
                                upload_reg_ts, 'scan')
                exec_wf.connect(merge_reg, 'out_file',
                                upload_reg_ts, 'in_files')

                # Pass the realigned time series to modeling.
                mdl_input_spec.inputs.resource = self.registration_resource
                exec_wf.connect(merge_reg, 'out_file',
                                mdl_wf, 'input_spec.time_series')
                self.logger.debug('Connected registration to modeling.')
            else:
                # Model the scan input.
                mdl_input_spec.inputs.resource = SCAN_TS_RSC
                exec_wf.connect(scan_ts, 'out_file',
                                mdl_wf, 'input_spec.time_series')

        # Set the configured workflow node inputs and plug-in options.
        self._configure_nodes(exec_wf)

        self.logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(exec_wf)

        return exec_wf

    def _run_workflow(self, workflow):
        """
        Overrides the superclass method to build the child workflows
        if the *dry_run* instance variable flag is set.

        :param workflow: the workflow to run
        """
        super(QIPipelineWorkflow, self)._run_workflow(workflow)
        if self.dry_run:
            # Make a dummy temp directory and files for simulating
            # the called workflows. These workflows inherit the
            # dry_run flag from this parent workflow and only go
            # through the motions of execution.
            dummy_dir = tempfile.mkdtemp()
            dummy_volume = "%s/volume001.nii.gz" % dummy_dir
            open(dummy_volume, 'a').close()
            _, dummy_roi = tempfile.mkstemp(dir=dummy_dir, pre='roi')
            _, dummy_ts = tempfile.mkstemp(dir=dummy_dir, pre='ts')
            opts = self._child_options()
            try:
                # If staging is enabled, then simulate it.
                if self.workflow.get_node('stage'):
                    input_spec = self.workflow.get_node('input_spec')
                    in_dirs = input_spec.inputs.in_dirs
                    stg_opts = dict(collection=self.collection.name,
                                    dest=dummy_dir, **opts)
                    stage('Breast001', 'Session01', 1, in_dirs, stg_opts)
                # If registration is enabled, then simulate it.
                if self.workflow.get_node('register'):
                    reg_opts = dict(technique='ANTs', **opts)
                    registration.run('Breast001', 'Session01', 1,
                                     dummy_volume, dummy_volume, **reg_opts)
                # If ROI is enabled, then simulate it.
                if self.workflow.get_node('roi'):
                    # A dummy (lesion, slice index, in_file) ROI input tuple.
                    inputs = [LesionROI(1, 1, 1, dummy_roi)]
                    roi('Breast001', 'Session01', 1, dummy_ts, inputs, opts)
            finally:
                shutil.rmtree(dummy_dir)


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
    return [location for location in in_files
            if os.path.split(location)[1] not in exclusions]


def bolus_arrival_index_or_zero(time_series):
    """
    Determines the bolus uptake. If it could not be determined,
    then the first time point is taken to be the uptake volume.

    :param time_series: the 4D time series image
    :return: the bolus arrival index, or zero if the arrival
        cannot be calculated
    """
    from qipipe.helpers.bolus_arrival import (bolus_arrival_index,
                                              BolusArrivalError)

    try:
        return bolus_arrival_index(time_series)
    except BolusArrivalError:
        return 0


def stage(subject, session, scan, in_dirs, opts):
    """
    Runs the staging workflow on the given session scan images.

    :Note: see the :meth:`register` note.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_dirs: the input DICOM directories
    :param opts: the :meth:`qipipe.pipeline.staging.run` keyword options
    :return: the 3D volume files
    """
    from qipipe.pipeline import staging

    return staging.run(subject, session, scan, *in_dirs, **opts)


def _register(subject, session, scan, reference_index, mask, in_files,
              opts):
    """
    A stub for the :meth:`register` method.

    :Note: The *mask* parameter is a registration option, but can't
      be included in the *opts* parameter, since it is an upstream
      workflow node connection point.

    :Note: contrary to Python convention, the opts method parameter
      is a required dictionary rather than a keyword aggregate (i.e.,
      ``**opts``). The Nipype ``Function`` interface does not support
      method aggregates. Similarly, the in_files parameter is a
      required list rather than a splat argument (i.e., *in_files).
    """
    from qipipe.pipeline.qipipeline import register

    return register(subject, session, scan, reference_index, *in_files,
                    mask=mask, **opts)


def register(subject, session, scan, reference_index, *in_files, **opts):
    """
    Runs the registration workflow on the given session scan images.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param reference_index: the index of the file to register against
    :param in_files: the input session scan 3D NIfTI images
    :param opts: the :meth:`qipipe.pipeline.registration.run` keyword
        options
    :return: the realigned image file path array
    """
    # Note: There is always a mask and resource argument. The mask
    # file and resource name are either specified as an input or
    # built by the workflow. The mask and resource are options in
    # the registration run function. Therefore, we check that the
    # options are set here.
    if not opts.get('mask'):
        raise PipelineError("The register method is missing the mask")
    if not opts.get('resource'):
        raise PipelineError("The register method is missing the"
                            " XNAT registration resource name")

    # The input scan files sorted by volume number.
    volumes = sorted(in_files, key=_extract_volume_number)
    # The initial fixed image.
    reference = volumes[reference_index]

    # Register the files after the reference point.
    ref_successor = reference_index + 1
    after = volumes[ref_successor:]
    post = registration.run(subject, session, scan, reference,
                            *after, **opts)
    # Register the files before the reference point in
    # reverse order in case the recursive flag is set.
    before = volumes[:reference_index]
    before.reverse()
    pre = registration.run(subject, session, scan, reference,
                           *before, **opts)
    # Restore the original sort order.
    pre.reverse()
    # The registration result in sort order.
    output = pre + [reference] + post

    # Infer the project from the options.
    prj_opt = opts.get('project')
    if prj_opt:
        project = prj_opt
    else:
        parent_wf = opts.get('parent')
        if parent_wf:
            project = parent_wf.project
        else:
            raise PipelineError("The registration project could not"
                                " be determined from the options")
    # Get the resource from the options.
    resource = opts.get('resource')
    if not resource:
        raise PipelineError("The registration resource option was not found")

    # Upload the registration profile and the unrealigned image into the
    # XNAT registration resource. The profile is in the same directory as
    # the realigned images.
    reg_dir, _ = os.path.split(output[ref_successor])
    prf_match = glob.glob("%s/*.cfg" % reg_dir)
    if not prf_match:
        raise PipelineError("The registration profile was not found in"
                            " the registration destination directory %s" %
                            reg_dir)
    if len(prf_match) > 1:
        raise PipelineError("More than one .cfg file was found in the"
                            " registration destination directory %s" %
                            reg_dir)
    profile = prf_match[0]
    _, prf_base = os.path.split(profile)
    _, ref_base = os.path.split(reference)
    in_files = [profile, reference]
    logger(__name__).debug(
        "Uploading the registration profile %s and fixed reference image"
        " %s to %s %s scan %d resource %s from %s..." %
        (prf_base, ref_base, subject, session, scan, resource, reg_dir)
    )
    upload = XNATUpload(project=project, subject=subject, session=session,
                        scan=scan, resource=resource, in_files=in_files,
                        modality='MR')
    upload.run()
    logger(__name__).info(
        "Uploaded the %s %s scan %d registration result to resource"
        " %s." % (subject, session, scan, resource)
    )

    # Return the registeration result.
    return output


def _extract_volume_number(location):
    _, fname = os.path.split(location)
    match = VOLUME_FILE_PAT.match(fname)
    if not match:
        raise ArgumentError("The volume file base name does not match"
                            " pattern %s: %s" %
                            (VOLUME_FILE_PAT.pattern, fname))
    return int(match.group(1))


def roi(subject, session, scan, time_series, in_rois, opts):
    """
    Runs the ROI workflow on the given session scan images.

    :Note: see the :meth:`register` note.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param time_series: the scan 4D time series
    :param in_rois: the :meth:`qipipe.pipeline.roi.run` input ROI specs
    :param opts: the :meth:`qipipe.pipeline.roi.run` keyword options
    :return: the ROI volume index
    """
    from qipipe.pipeline import roi

    roi.run(subject, session, scan, time_series, *in_rois, **opts)

    # Get the ROI volume index from any input spec.
    roi_volume_nbr = in_rois[0].volume

    # Return the volume index.
    return roi_volume_nbr - 1
