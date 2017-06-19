import os
import re
import glob
import shutil
import tempfile
import logging
import six
# The ReadTheDocs build does not include nipype.
on_rtd = os.environ.get('READTHEDOCS') == 'True'
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
    iter_opts = dict(scan=opts['scan']) if 'scan' in opts else {}
    if actions == ['roi']:
        iter_opts['skip_existing'] = False
    for scan_input in iter_stage(project, collection, *inputs, **iter_opts):
        wf_actions = _filter_actions(scan_input, actions)
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
            project, scan_input.subject, scan_input.session, scan_input.scan,
            wf_actions, collection=collection, dest=scan_dest,
            base_dir=scan_base_dir, **opts)
        # Run the workflow on the scan.
        workflow.run_with_dicom_input(wf_actions, scan_input)

    # If staging is enabled, then make the TCIA subject map.
    if 'stage' in actions:
        map_ctp(collection, *subjects, dest=dest)


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
        # The XNAT connection is open while the input scan is processed.
        with qixnat.connect() as xnat:
            # The XNAT scan object must exist.
            scan_obj = xnat.find_one(prj, sbj, sess, scan=scan)
            if not scan_obj:
                raise PipelineError("The XNAT scan object does not exist: %s" %
                                    path)

        # Make the workflow.
        workflow = QIPipelineWorkflow(prj, sbj, sess, scan, actions, **opts)

        # Run the workflow.
        workflow.run_with_scan_download(prj, sbj, sess, scan, actions)



def _scan_file_exists(xnat, project, subject, session, scan, resource,
                      file_pat=None):
    """
    :param file_pat: the optional target XNAT label pattern to match
        (default any file)
    :return: whether the given XNAT scan file exists
    """
    matches = _scan_files(xnat, project, subject, session, scan,
                          resource, file_pat)

    return not not matches

def _scan_files(xnat, project, subject, session, scan, resource,
                file_pat=None):
    """
    :param file_pat: the optional target XNAT file label pattern to match
        (default any file)
    :return: the XNAT scan file name list
    """
    rsc_obj = xnat.find_one(project, subject, session, scan=scan,
                            resource=resource)
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
            (project, subject, session, scan, resource, len(matches),
             file_pat.pattern)
        )
        return matches
    else:
        logger(__name__).debug(
            "The %s %s %s scan %d resource %s contains %d files." %
            (project, subject, session, scan, resource, len(files))
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

    def __init__(self, project, subject, session, scan, actions, **opts):
        """
        :param project: the XNAT project name
        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
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
            __name__, project=project, **opts
        )

        collOpt = opts.pop('collection', None)
        if collOpt:
            self.collection = image_collection.with_name(collOpt)
        else:
            self.collection = None

        # Capture the registration resource name, or generate if
        # necessary. The registration resource name is created
        # here rather than by the registration workflow, since
        # it the registration time series is built and uploaded
        # in the supervisory workflow.
        reg_rsc_opt = opts.pop('registration_resource', None)
        if reg_rsc_opt:
            reg_rsc = reg_rsc_opt
        elif 'register' in actions:
            reg_rsc = registration.generate_resource_name()
            self.logger.debug(
                "Generated %s %s scan %d registration resource name %s." %
                (subject, session, scan, reg_rsc)
            )
        else:
            reg_rsc = None
        self.registration_resource = reg_rsc
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

        self.workflow = self._create_workflow(subject, session, scan,
                                              actions, **opts)
        """
        The pipeline execution workflow. The execution workflow is executed
        by calling the :meth:`run_with_dicom_input` or
        :meth:`run_with_scan_download` method.
        """

    def run_with_dicom_input(self, actions, scan_input):
        """
        :param actions: the workflow actions to perform
        :param scan_input: the {subject, session, scan, dicom, roi}
            object
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
        input_spec.inputs.registered = []

        # If roi is enabled and has input, then set the roi function inputs.
        if 'roi' in actions:
            roi_dirs = scan_input.roi
            if roi_dirs:
                scan_pats = self.collection.patterns.scan[scan_input.scan]
                if not scan_pats:
                    raise PipelineError("Scan patterns were not found"
                                        " for %s %s scan %d" % (
                                        scan_input.subject, scan_input.session,
                                        scan_input.scan))
                glob = scan_pats.roi.glob
                regex = scan_pats.roi.regex
                self.logger.debug(
                    "Discovering %s %s scan %d ROI files matching %s..." %
                    (scan_input.subject, scan_input.session, scan_input.scan,
                     glob)
                )
                roi_inputs = list(iter_roi(regex, *roi_dirs))
                if roi_inputs:
                    self.logger.info(
                        "%d %s %s scan %d ROI files were discovered." %
                        (len(roi_inputs), scan_input.subject,
                         scan_input.session, scan_input.scan)
                    )
                else:
                    self.logger.info("No ROI file was detected for"
                                      " %s %s scan %d." %
                                      (scan_input.subject, scan_input.session,
                                       scan_input.scan))
                # Set the inputs even if none are found. This permits
                # workflow "execution" of the roi node as a no-op and
                # avoids a TypeError from the roi() function call.
                self._set_roi_inputs(*roi_inputs)
            else:
                self.logger.info("ROI directory was not detected for"
                                  " %s %s scan %d." %
                                  (scan_input.subject, scan_input.session,
                                   scan_input.scan))
                # Set the empty inputs for a no-op workflow.
                self._set_roi_inputs()

        # Execute the workflow.
        self.logger.info("Running the pipeline on %s %s scan %d." %
                           (scan_input.subject, scan_input.session,
                            scan_input.scan))
        self._run_workflow(self.workflow)
        self.logger.info("Completed pipeline execution on %s %s scan %d." %
                           (scan_input.subject, scan_input.session,
                            scan_input.scan))

    def run_with_scan_download(self, project, subject, session, scan, actions):
        """
        Runs the execution workflow on downloaded scan image files.

        :param project: the project name
        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param actions: the workflow actions
        """
        self.logger.debug("Processing the %s %s %s scan %d volumes..." %
                           (project, subject, session, scan))
        with qixnat.connect() as xnat:
            # The XNAT volume file names.
            scan_volumes = _scan_files(xnat, project, subject, session, scan,
                                       'NIFTI', VOLUME_FILE_PAT)
            if not scan_volumes:
                raise PipelineError("There are no pipeline %s %s %s"
                                    " scan %d volumes" %
                                    (project, subject, session, scan))

            registered, unregistered = self._partition_registered(
                xnat, project, subject, session, scan, scan_volumes
            )

        # Validate and log the partition.
        if 'register' in actions:
            if registered:
                if unregistered:
                    self.logger.debug(
                        "Skipping registration of %d previously"
                        " registered %s %s scan %d volumes:" %
                        (len(registered), subject, session, scan)
                    )
                    self.logger.debug("%s" % registered)
                else:
                    self.logger.debug(
                        "Skipping %s %s scan %d registration, since"
                        " all volumes are already registered." %
                        (subject, session, scan)
                    )
            if unregistered:
                self.logger.debug(
                    "Registering the following %d %s %s scan %d volumes:" %
                    (len(unregistered), subject, session, scan)
                )
                self.logger.debug("%s" % unregistered)
        elif unregistered and self.registration_resource:
            raise PipelineError(
                "The pipeline %s %s scan %d register action is not"
                " specified but there are %d unregistered scan volumes"
                " not in the %s registration resource" %
                (subject, session, scan, len(unregistered),
                 self.registration_resource)
            )
        elif registered:
            self.logger.debug("Processing %d %s %s scan %d volumes:" %
                              (len(registered), subject, session, scan))
            self.logger.debug("%s" % registered)

        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        input_spec.inputs.registered = registered

        # Execute the workflow.
        self._run_workflow(self.workflow)

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
        reg_obj = xnat.find_one(project, subject, session, scan=scan,
                                resource=self.registration_resource)

        # The realigned files.
        reg_set = set(reg_obj.files().get()) if reg_obj else set()
        # The unregistered files.
        unreg_set = set(files) - reg_set
        registered = sorted(reg_set)
        unregistered = sorted(unreg_set)
        self.logger.debug("The %s %s scan %d resource has %d registered volumes"
                           " and %d unregistered volumes." %
                           (subject, session, scan, len(registered),
                            len(unregistered)))

        return registered, unregistered

    def _create_workflow(self, subject, session, scan, actions, **opts):
        """
        Builds the reusable pipeline workflow described in
        :class:`qipipe.pipeline.qipipeline.QIPipeline`.

        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
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

        # The technique options.
        reg_tech_opt = opts.get('registration_technique')
        recursive_reg_opt = opts.get('recursive_registration')
        mdl_tech_opt = opts.get('modeling_technique')

        if 'model' in actions:
            mdl_flds = ['subject', 'session', 'scan', 'time_series',
                        'mask', 'bolus_arrival_index', 'opts']
            mdl_xfc = Function(input_names=mdl_flds,
                               output_names=['results'],
                               function=model)
            mdl_node = pe.Node(mdl_xfc, name='model')
            mdl_opts = self._child_options()
            mdl_opts['technique'] = self.modeling_technique
            mdl_node.inputs.opts = mdl_opts
            self.logger.info("Enabled modeling with options %s." % mdl_opts)
        else:
            mdl_node = None

        # The registration workflow node.
        if 'register' in actions:
            reg_inputs = ['subject', 'session','scan', 'reference_index',
                          'mask', 'in_files', 'opts']

            # The registration technique option is required
            # for the registration action.
            if not self.registration_technique:
                raise PipelineError('Missing the registration technique')

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

            # The fixed reference volume index option.
            reg_ref_opt = opts.pop('registration_reference', None)
            if reg_ref_opt:
                reg_node.inputs.reference_index = int(reg_ref_opt) - 1
                self.logger.debug("The registration reference is %d." %
                                  reg_node.inputs.reference_index)

            self.logger.info("Enabled registration with options %s." %
                             reg_opts)
        else:
            self.logger.info("Skipping registration.")
            reg_node = None

        # The ROI workflow node.
        if 'roi' in actions:
            roi_flds = ['subject', 'session', 'scan', 'time_series',
                          'in_rois', 'opts']
            roi_xfc = Function(input_names=roi_flds,
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

        # The staging workflow.
        if 'stage' in actions:
            stg_inputs = ['subject', 'session', 'scan', 'in_dirs', 'opts']
            stg_xfc = Function(input_names=stg_inputs,
                               output_names=['time_series', 'volume_files'],
                               function=stage)
            stg_node = pe.Node(stg_xfc, name='stage')
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
            stg_node.inputs.opts = stg_opts
            self.logger.info("Enabled staging with options %s" % stg_opts)
        else:
            stg_node = None
            self.logger.info("Skipping staging.")

        # Validate that there is at least one constituent workflow.
        if not any([stg_node, roi_node, reg_node, mdl_node]):
            raise PipelineError("No workflow was enabled.")

        # Registration and modeling require a mask.
        is_mask_required = (
            (reg_node and self.registration_technique != 'Mock') or
            (mdl_node and self.modeling_technique != 'Mock')
        )
        if is_mask_required:
            has_mask = False
            # If volumes are already staged, then check for an
            # existing XNAT mask.
            if not stg_node:
                with qixnat.connect() as xnat:
                    has_mask = _scan_file_exists(
                        xnat, self.project, subject, session, scan,
                        MASK_RESOURCE, MASK_FILE
                    )
            if has_mask:
                dl_mask_xfc = XNATDownload(project=self.project,
                                           resource=MASK_RESOURCE,
                                           file=MASK_FILE)
                mask_node = pe.Node(dl_mask_xfc, name='download_mask')
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
                                    function=mask)
                mask_node = pe.Node(mask_xfc, name='mask')
                mask_node.inputs.opts = mask_opts
                self.logger.info("Enabled scan mask creation with options"
                                 " %s." % mask_opts)
        else:
            mask_node = None
            self.logger.info("Skipping scan mask creation.")

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
        # Modeling requires a volume iterator node if and only if
        # modeling is performed on the scan and the scan time series
        # is not available.

        # Stitch together the workflows:

        # If staging is enabled, then stage the DICOM input.
        if stg_node:
            for field in input_spec.inputs.copyable_trait_names():
                exec_wf.connect(input_spec, field, stg_node, field)

        # The mask, ROI and scan modeling downstream actions require
        # a scan time series. If there is a scan time series resource
        # option, then the scan time series will be downloaded.
        # Otherwise, it will be created from the staged input.
        is_scan_modeling = (
            mdl_node and not reg_node and not self.registration_resource
        )
        need_scan_ts = mask_node or roi_node or is_scan_modeling
        if need_scan_ts:
            if stg_node:
                scan_ts = stg_node
            else:
                dl_scan_ts_xfc = XNATDownload(project=self.project,
                                              resource='NIFTI',
                                              file=SCAN_TS_FILE)
                dl_scan_ts = pe.Node(dl_scan_ts_xfc,
                                  name='download_scan_time_series')
                exec_wf.connect(input_spec, 'subject', dl_scan_ts, 'subject')
                exec_wf.connect(input_spec, 'session', dl_scan_ts, 'session')
                exec_wf.connect(input_spec, 'scan', dl_scan_ts, 'scan')
                # Rename the download out_file to volume_files.
                scan_ts_xfc = IdentityInterface(fields=['time_series'])
                scan_ts_xfc = pe.Node(scan_ts_xfc)
                exec_wf.connect(dl_scan_ts, 'out_file',
                                scan_ts_xfc, 'time_series')

        # Registration and the scan time series require a staged
        # node scan with output 'images'. If staging is enabled,
        # then staged is the stg_node. Otherwise, the staged node
        # downloads the previously uploaded scan volumes.
        #
        # The scan time series is required by mask and scan
        # registration.
        scan_volumes = None
        if reg_node:
            if stg_node:
                scan_volumes = stg_node
            else:
                dl_vols_xfc = XNATDownload(project=self.project,
                                           resource='NIFTI',
                                           file='volume*.nii.gz')
                dl_vols_node = pe.Node(dl_vols_xfc, name='scan_volumes')
                exec_wf.connect(input_spec, 'subject', dl_vols_node, 'subject')
                exec_wf.connect(input_spec, 'session', dl_vols_node, 'session')
                exec_wf.connect(input_spec, 'scan', dl_vols_node, 'scan')
                # Rename the download out_file to volume_files.
                scan_volumes_xfc = IdentityInterface(fields=['volume_files'])
                scan_volumes = pe.Node(scan_volumes_xfc)
                exec_wf.connect(dl_vols_node, 'out_files',
                                dl_vols_node, 'volume_files')

        # Registration and modeling require a mask and bolus arrival.
        if mask_node:
            exec_wf.connect(input_spec, 'subject', mask_node, 'subject')
            exec_wf.connect(input_spec, 'session', mask_node, 'session')
            exec_wf.connect(input_spec, 'scan', mask_node, 'scan')
            if hasattr(mask_node.inputs, 'time_series'):
                exec_wf.connect(scan_ts, 'time_series',
                                mask_node, 'time_series')
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
                reg_node and not roi_node
                and not reg_node.inputs.reference_index
            )
            is_bolus_arrival_required = (
                compute_reg_reference or
                (mdl_node and self.modeling_technique != 'Mock')
            )
            # Modeling always requires the bolus arrival.
            bolus_arv_node = None
            if is_bolus_arrival_required:
                # Compute the bolus arrival from the scan time series.
                bolus_arv_xfc = Function(input_names=['time_series'],
                                         output_names=['bolus_arrival_index'],
                                         function=bolus_arrival_index_or_zero)
                bolus_arv_node = pe.Node(bolus_arv_xfc,
                                         name='bolus_arrival_index')
                exec_wf.connect(scan_ts, 'time_series',
                                bolus_arv_node, 'time_series')
                self.logger.debug('Connected the scan time series to the bolus'
                                  ' arrival calculation.')

        # If ROI is enabled, then convert the ROIs using the scan
        # time series.
        if roi_node:
            exec_wf.connect(input_spec, 'subject', roi_node, 'subject')
            exec_wf.connect(input_spec, 'session', roi_node, 'session')
            exec_wf.connect(input_spec, 'scan', roi_node, 'scan')
            exec_wf.connect(scan_ts, 'time_series', roi_node, 'time_series')
            self.logger.debug('Connected the scan time series to ROI.')

        # If registration is enabled, then register the unregistered
        # staged images.
        if reg_node:
            # There must be staged files.
            if not scan_volumes:
                raise NotFoundError('Registration requires a scan input')
            exec_wf.connect(input_spec, 'subject', reg_node, 'subject')
            exec_wf.connect(input_spec, 'session', reg_node, 'session')
            exec_wf.connect(input_spec, 'scan', reg_node, 'scan')

            # If the registration input files were downloaded from
            # XNAT, then select only the unregistered files.
            if stg_node:
                exec_wf.connect(scan_volumes, 'volume_files',
                                reg_node, 'in_files')
            else:
                exc_regd_xfc = Function(input_names=['in_files', 'exclusions'],
                                        output_names=['out_files'],
                                        function=exclude_files)
                exclude_registered = pe.Node(exc_regd_xfc,
                                             name='exclude_registered')
                exec_wf.connect(scan_volumes, 'volume_files',
                                exclude_registered, 'in_files')
                exec_wf.connect(input_spec, 'registered',
                                exclude_registered, 'exclusions')
                exec_wf.connect(exclude_registered, 'out_files',
                                reg_node, 'in_files')
            self.logger.debug('Connected staging to registration.')

            # The mask input.
            if mask_node:
                exec_wf.connect(mask_node, 'out_file', reg_node, 'mask')
                self.logger.debug('Connected the mask to registration.')

            # If the ROI workflow is enabled, then register against
            # the ROI volume. Otherwise, use the bolus arrival volume.
            if not reg_node.inputs.reference_index:
                if roi_node:
                    exec_wf.connect(roi_node, 'volume',
                                    reg_node, 'reference_index')
                    self.logger.debug('Connected ROI volume to the'
                                      ' registration reference index.')
                elif bolus_arv_node:
                    exec_wf.connect(bolus_arv_node, 'bolus_arrival_index',
                                    reg_node, 'reference_index')
                    self.logger.debug('Connected bolus arrival to the'
                                      ' registration reference index.')

            # Stack the registered images into a 4D time series.
            reg_ts_name = self.registration_resource + '_ts'
            merge_reg_xfc = MergeNifti(out_format=reg_ts_name)
            merge_reg = pe.Node(merge_reg_xfc, name='merge_reg_volumes')
            exec_wf.connect(reg_node, 'out_files', merge_reg, 'in_files')

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

        # If the modeling workflow is enabled, then model the scan or
        # realigned images.
        if mdl_node:
            exec_wf.connect(input_spec, 'subject', mdl_node, 'subject')
            exec_wf.connect(input_spec, 'session', mdl_node, 'session')
            exec_wf.connect(input_spec, 'scan', mdl_node, 'scan')
            # The mask input.
            if mask_node:
                exec_wf.connect(mask_node, 'out_file', mdl_node, 'mask')
                self.logger.debug('Connected the mask to modeling.')
            # The bolus arrival input.
            if bolus_arv_node:
                exec_wf.connect(bolus_arv_node, 'bolus_arrival_index',
                                mdl_node, 'bolus_arrival_index')
            self.logger.debug('Connected bolus arrival to modeling.')

            # Obtain the modeling input 4D time series.
            if is_scan_modeling:
                # There is no register action and no registration
                # resource option. In that case, model the scan
                # input. scan_ts is always created previously if
                # is_scan_modeling is true.
                exec_wf.connect(scan_ts, 'time_series', mdl_node, 'time_series')
            elif reg_node:
                # merge_reg is created in the registration processing.
                exec_wf.connect(merge_reg, 'out_file', mdl_node, 'time_series')
                self.logger.debug('Connected registration to modeling.')
            else:
                # Check for a previously created registration time
                # series. Note that self.registration_resource is
                # set since is_scan_modeling is false and reg_node
                # is None.
                reg_ts_name = self.registration_resource + '_ts.nii.gz'
                with qixnat.connect() as xnat:
                    has_reg_ts = _scan_file_exists(
                        xnat, self.project, subject, session, scan,
                        self.registration_resource, reg_ts_name
                    )
                # The time series must have been created by the
                # registration process.
                if not has_reg_ts:
                    raise PipelineError(
                        "The %s %s scan %d registration resource %s does"
                        " not have the time series file %s" %
                        (subject, session. scan, self.registration_resource,
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
                exec_wf.connect(dl_reg_ts, 'out_file', mdl_node, 'time_series')

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
                    stage('Breast001', 'Session01', 1, in_dirs, stg_opts)
                # If registration is enabled, then simulate it.
                if self.workflow.get_node('register'):
                    reg_opts = dict(technique=self.registration_technique,
                                    **opts)
                    registration.run('Breast001', 'Session01', 1,
                                     dummy_volume, dummy_volume, **reg_opts)
                # If ROI is enabled, then simulate it.
                if self.workflow.get_node('roi'):
                    # A dummy (lesion, slice index, in_file) ROI input tuple.
                    inputs = [LesionROI(1, 1, 1, dummy_roi)]
                    roi('Breast001', 'Session01', 1, dummy_ts, inputs, opts)
                # If modeling is enabled, then simulate it.
                if self.workflow.get_node('model'):
                    mdl_opts = dict(technique=self.modeling_technique,
                                    bolus_arrival_index=0, **opts)
                    modeling.run('Breast001', 'Session01', 1, dummy_ts,
                                 **mdl_opts)
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
    return [f for f in in_files
            if os.path.split(f)[1] not in exclusions]


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

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_dirs: the input DICOM directories
    :param opts: the :meth:`qipipe.pipeline.staging.run` keyword options
    :return: the :meth:`qipipe.staging.run` result
    """
    from qipipe.pipeline import staging

    return staging.run(subject, session, scan, *in_dirs, **opts)


def _register(subject, session, scan, in_files, opts,
              reference_index=0, mask=None):
    """
    A facade for the :meth:`qipipe.pipeline.registration.register
    method.

    :Note: The *mask* and *reference_index* parameters are
      registration options, but can't be included in the *opts*
      parameter, since they are potential upstream workflow node
      connection points. Since a mock registration technique
      does not connect these inputs, they have default values
      in the method signature as well.

    :Note: contrary to Python convention, the *opts* method
      parameter is a required dictionary rather than a keyword
      double-splat argument (i.e., ``**opts``). The Nipype
      ``Function`` interface does not support double-splat
      arguments. Similarly, the *in_files* parameter is a list
      rather than a splat argument (i.e., *in_files).

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_files: the input session scan 3D NIfTI images
    :param opts: the :meth:`qipipe.pipeline.registration.run`
        keyword options
    :param reference_index: the zero-based index of the file to
        register against (default first volume)
    :param mask: the mask file, required unless the model
        technique is ``Mock``
    """
    from qipipe.pipeline.qipipeline import register

    return register(subject, session, scan, reference_index,
                    *in_files, mask=mask, **opts)


def register(subject, session, scan, reference_index, *in_files, **opts):
    """
    Runs the registration workflow on the given session scan images.

    :Note: There is always a mask and resource argument. The mask
      file and resource name are either specified as an input or
      built by the workflow. The mask and resource are options in
      the registration run function. Therefore, we check that these
      options are set here.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param reference_index: the zero-based index of the file to
        register against
    :param in_files: the input session scan 3D NIfTI images
    :param opts: the :meth:`qipipe.pipeline.registration.run` keyword
        options
    :return: the realigned image file path array
    """
    if not opts.get('mask'):
        raise PipelineError("The register method is missing the mask")
    if not opts.get('resource'):
        raise PipelineError("The register method is missing the"
                            " XNAT registration resource name")

    # The input scan files sorted by volume number.
    volumes = sorted(in_files, key=_extract_volume_number)
    # The initial fixed image.
    reference = volumes[reference_index]

    _logger = logger(__name__)
    _logger.debug(
        "Registering %d volumes against the reference volume %s..." %
         (len(in_files), reference)
    )

    # Register the files after the reference point.
    ref_successor = reference_index + 1
    after = volumes[ref_successor:]
    if after:
        _logger.debug(
            "Registering the %d volumes after the fixed reference"
            " volume %s..." % (len(after), reference)
        )
        post = registration.run(subject, session, scan, reference,
                                *after, **opts)
        _logger.debug(
            "Registered the %d volumes after the fixed reference"
            " volume %s." % (len(after), reference)
        )
    else:
        post = []

    # Register the files before the reference point in
    # reverse order in case the recursive flag is set.
    before = volumes[:reference_index]
    before.reverse()
    if before:
        _logger.debug(
            "Registering the %d volumes before the fixed reference"
            " volume %s..." % (len(before), reference)
        )
        pre = registration.run(subject, session, scan, reference,
                               *before, **opts)
        _logger.debug(
            "Registered the %d volumes before the fixed reference"
            " volume %s." % (len(before), reference)
        )
        # Restore the original sort order.
        pre.reverse()
    else:
        pre = []

    _logger.debug("Registered %d volumes." % len(in_files))

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
            raise PipelineError('The registration project could not be'
                                ' determined from the options')
    # Get the resource from the options.
    resource = opts.get('resource')
    if not resource:
        raise PipelineError('The registration resource option was not found')

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
    _logger.debug(
        "Uploading the registration profile %s and fixed reference image"
        " %s to %s %s scan %d resource %s from %s..." %
        (prf_base, ref_base, subject, session, scan, resource, reg_dir)
    )
    upload = XNATUpload(project=project, subject=subject, session=session,
                        scan=scan, resource=resource, in_files=in_files,
                        modality='MR')
    upload.run()
    _logger.info(
        "Uploaded the %s %s scan %d registration result to resource"
        " %s." % (subject, session, scan, resource)
    )

    # Return the registration result.
    return output


def _extract_volume_number(location):
    _, base_name = os.path.split(location)
    match = VOLUME_FILE_PAT.match(base_name)
    if not match:
        raise PipelineError("The volume file base name does not match"
                            " pattern %s: %s" %
                            (VOLUME_FILE_PAT.pattern, base_name))
    return int(match.group(1))


def mask(subject, session, scan, time_series, opts):
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
    :return: the zero-based ROI volume index
    """
    from qipipe.pipeline import roi
    from qipipe.helpers.logging import logger

    # If there are no ROI inputs, then call roi.run() anyway to
    # create the workflow and print appropriate log messages.
    # The roi.run() method will return None in that case, and
    # we will bail out without determining the ROI volume
    # number.
    if not roi.run(subject, session, scan, time_series, *in_rois, **opts):
        logger(__name__).debug(
            "%s %s scan %d does not have ROI input files; using the"
            " default registration reference volume index 0 to allow"
            " Nipype to continue running any downstream actions." %
            (subject, session, scan)
        )
        return 0

    # Get the ROI volume index from any input spec.
    roi_volume_nbr = in_rois[0].volume

    # Return the volume index.
    return roi_volume_nbr - 1


def model(subject, session, scan, time_series, bolus_arrival_index,
          opts, mask=None):
    """
    Runs the modeling workflow on the given time series.
    *mask* and *bolus_arrival_index* are
    :class:`qipipe.pipeline.modeling.ModelingWorkflow` options,
    but are required input to this ``model`` function.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param time_series: the scan or registration 4D time series
    :param bolus_arrival_index: the required bolus arrival index
    :param opts: the :meth:`qipipe.pipeline.modeling.run` keyword options
    :param mask: the mask file, required unless the model technique
        is ``Mock``
    :return: the modeling result dictionary
    """
    from qipipe.pipeline import modeling

    return modeling.run(subject, session, scan, time_series,
                        mask=mask, bolus_arrival_index=bolus_arrival_index,
                        **opts)
