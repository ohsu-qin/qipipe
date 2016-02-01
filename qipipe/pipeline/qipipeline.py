import os
import re
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
from qiutil.logging import logger
from . import (staging, registration, modeling)
from .pipeline_error import PipelineError
from .workflow_base import WorkflowBase
from .staging import StagingWorkflow
from ..staging import image_collection
from ..staging.iterator import iter_stage
from ..staging.map_ctp import map_ctp
from ..staging.ohsu import MULTI_VOLUME_SCAN_NUMBERS
from .modeling import (ModelingWorkflow, MODELING_CONF_FILE)
from .mask import MaskWorkflow
from .roi import ROIWorkflow
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
            raise ArgumentError("The ROI pipeline can only be run with"
                                " staging or stand-alone")
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
        raise ArgumentError('The staging pipeline collection option is missing.')
    # The required XNAT project name.
    project = opts.pop('project', None)
    if not project:
        raise ArgumentError('The staging pipeline project option is missing.')
    # The target directory.
    dest = opts.get('dest', None)
    
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
        wf_gen.run_with_dicom_input(wf_actions, scan_input, dest)
    
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
    
    In addition, if the staging or registration workflow is enabled
    then the *iter_volume* node iterables input includes the
    following fields:
    
    - *volume*: the scan number
    
    - *dest*: the target staging directory, if the staging
      workflow is enabled
    
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
        :keyword collection: the image collection name
        :keyword mask: the XNAT mask resource name
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
        """
        super(QIPipelineWorkflow, self).__init__(
            project=project, logger=logger(__name__), **opts
        )
        
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
    
    def run_with_dicom_input(self, actions, scan_input, dest=None):
        """
        :param actions: the workflow actions to perform
        :param scan_input: the :class:`qipipe.staging.iterator.ScanInput`
            object
        :param dest: the TCIA staging destination directory (default is
            the current working directory)
        """
        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.collection = scan_input.collection
        input_spec.inputs.subject = scan_input.subject
        input_spec.inputs.session = scan_input.session
        input_spec.inputs.scan = scan_input.scan
        input_spec.inputs.registered = []
        
        # If staging is enabled, then set the staging iterables.
        if 'stage' in actions:
            staging.set_workflow_iterables(self.workflow, scan_input, dest)
        
        # If roi is enabled and has input, then set the roi function inputs.
        if 'roi' in actions and scan_input.iterators.roi:
            self._set_roi_inputs(*scan_input.iterators.roi)
        # Execute the workflow.
        self.logger.debug("Running the pipeline on %s %s scan %d." %
                           (scan_input.subject, scan_input.session,
                            scan_input.scan))
        self._run_workflow(self.workflow)
        self.logger.debug("Completed pipeline execution on %s %s scan %d." %
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
        roi_node.inputs.opts = dict(base_dir=self.base_dir)
    
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
        
        if 'model' in actions:
            mdl_wf_gen = ModelingWorkflow(parent=self, technique=mdl_tech_opt,
                                          resource=self.modeling_resource)
            mdl_wf = mdl_wf_gen.workflow
            self.modeling_resource = mdl_wf_gen.resource
        else:
            mdl_wf = None
        
        # The registration workflow node.
        if 'register' in actions:
            reg_inputs = ['technique', 'project', 'subject', 'session',
                          'scan', 'resource', 'bolus_arrival_index',
                          'in_files', 'mask', 'opts']
            # The registration function keyword options.
            if not self.registration_technique:
                raise ArgumentError('Missing the registration technique')
            if not self.registration_resource:
                raise ArgumentError('Missing the registration resource name')
            # Spell out the registration workflow options rather
            # than delegating to this qipipeline workflow as the
            # parent, since Nipype Function arguments must be
            # primitive.
            reg_opts = dict(
                base_dir=self.base_dir,
                distributable=self.is_distributable,
                dry_run=self.dry_run
            )
            if 'recursive_registration' in opts:
                reg_opts['recursive'] = opts['recursive_registration']
            # The registration function.
            reg_xfc = Function(input_names=reg_inputs,
                               output_names=['out_files'],
                               function=_register)
            reg_node = pe.Node(reg_xfc, name='register')
            reg_node.inputs.project = self.project
            reg_node.inputs.technique = self.registration_technique
            reg_node.inputs.resource = self.registration_resource
            reg_node.inputs.opts = reg_opts
            self.logger.info("Enabled registration.")
        else:
            self.logger.info("Skipping registration.")
            reg_node = None
        
        # The ROI workflow node.
        if 'roi' in actions:
            roi_inputs = ['project', 'subject', 'session', 'scan',
                          'time_series', 'in_rois', 'opts']
            roi_xfc = Function(input_names=roi_inputs, output_names=[],
                               function=roi)
            roi_node = pe.Node(roi_xfc, name='roi')
            roi_node.inputs.project = self.project
            roi_node.opts = dict(base_dir=self.base_dir,
                                 distributable=self.is_distributable)
            self.logger.info("Enabled ROI conversion.")
        else:
            roi_node = None
            self.logger.info("Skipping ROI conversion.")
        
        # Registration and modeling require a mask.
        if (reg_node or mdl_wf) and not mask_rsc_opt:
            collection_opt = opts.get('collection')
            if not collection_opt:
                raise ArgumentError("The mask workflow collection option is"
                                    " missing")
            coll = image_collection.with_name(collection_opt)
            crop_posterior = coll.crop_posterior
            mask_wf_gen = MaskWorkflow(parent=self,
                                       crop_posterior=crop_posterior)
            mask_wf = mask_wf_gen.workflow
            self.logger.info("Enabled scan mask creation.")
        else:
            mask_wf = None
            self.logger.info("Skipping scan mask creation.")
        
        # The staging workflow.
        if 'stage' in actions:
            stg_wf_gen = StagingWorkflow(parent=self)
            stg_wf = stg_wf_gen.workflow
            self.logger.info("Enabled staging.")
        else:
            stg_wf = None
            self.logger.info("Skipping staging.")
        
        # Validate that there is at least one constituent workflow.
        if not any([stg_wf, roi_node, reg_node, mdl_wf]):
            raise ArgumentError("No workflow was enabled.")
        
        # The workflow input fields.
        input_fields = ['subject', 'session', 'scan']
        iter_volume_fields = ['volume']
        # The staging workflow has additional input fields.
        # Partial registration requires the unregistered volumes input.
        if stg_wf:
            input_fields.append('collection')
            iter_volume_fields.append('dest')
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
        if stg_wf or reg_node or mask_wf or (mdl_wf and model_vol):
            iter_volume_xfc = IdentityInterface(fields=iter_volume_fields)
            iter_volume = pe.Node(iter_volume_xfc, name='iter_volume')
        # Staging requires a DICOM iterator node.
        if stg_wf:
            iter_dicom_xfc = IdentityInterface(fields=['volume', 'dicom_file'])
            iter_dicom = pe.Node(iter_dicom_xfc, name='iter_dicom')
            exec_wf.connect(iter_volume, 'volume', iter_dicom, 'volume')
        
        # Stitch together the workflows:
        
        # If staging is enabled, then stage the DICOM input.
        if stg_wf:
            for field in input_spec.inputs.copyable_trait_names():
                exec_wf.connect(input_spec, field,
                                stg_wf, 'input_spec.' + field)
            for field in iter_volume.inputs.copyable_trait_names():
                exec_wf.connect(iter_volume, field,
                                stg_wf, 'iter_volume.' + field)
            exec_wf.connect(iter_dicom, 'dicom_file',
                            stg_wf, 'iter_dicom.dicom_file')
        
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
            if stg_wf:
                staged = pe.JoinNode(IdentityInterface(fields=['out_files']),
                                    joinsource='iter_volume', name='staged')
                exec_wf.connect(stg_wf, 'output_spec.image',
                                staged, 'out_files')
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
                coll = image_collection.with_name(collection)
                # The volume grouping tag.
                vol_tag = coll.patterns.volume
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
            
            # Compute the bolus arrival from the scan time series.
            bolus_arv_xfc = Function(input_names=['time_series'],
                                     output_names=['bolus_arrival_index'],
                                     function=bolus_arrival_index_or_zero)
            bolus_arv = pe.Node(bolus_arv_xfc, name='bolus_arrival_index')
            exec_wf.connect(scan_ts, 'out_file', bolus_arv, 'time_series')
            self.logger.debug('Connected scan time series to bolus arrival.')
        
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
            if stg_wf:
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
            # The bolus arrival.
            exec_wf.connect(bolus_arv, 'bolus_arrival_index',
                            reg_node, 'bolus_arrival_index')
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
                upload_reg_ts_xfc = XNATUpload(project=self.project,
                                               resource=self.registration_resource,
                                               modality='MR')
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
        Overrides the superclass method to build the workflow if the
        *dry_run* instance variable flag is set.
        
        :param workflow: the workflow to run
        """
        super(QIPipelineWorkflow, self)._run_workflow(workflow)
        if self.dry_run:
            # Make a dummy empty file for simulating called workflows.
            _, path = tempfile.mkstemp()
            try:
                # If registration is enabled, then simulate it.
                if self.workflow.get_node('register'):
                    opts = dict(base_dir=self.workflow.base_dir, dry_run=True)
                    register('Dummy', 'Dummy', 'Dummy', 1, 0, path, [path],
                             opts)
                # If ROI is enabled, then simulate it.
                if self.workflow.get_node('roi'):
                    opts = dict(base_dir=self.workflow.base_dir, dry_run=True)
                    # A dummy (lesion, slice index, in_file) ROI input tuple.
                    inputs = [(1, 1, path)]
                    roi('Dummy', 'Dummy', 'Dummy', 1, path, inputs, opts)
            finally:
                os.remove(path)


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


def _register(technique, project, subject, session, scan, resource,
             bolus_arrival_index, mask, in_files, opts):
    """
    A stub for the :meth:`register` method.
    
    :Note: contrary to Python convention, the opts method parameter
      is a required dictionary rather than a keyword aggregate (i.e.,
      ``**opts``). The Nipype ``Function`` interface does not support
      method aggregates. Similarly, the in_files parameter is a
      required list rather than a splat argument (i.e., *in_files).
    """
    from qipipe.pipeline.qipipeline import register
    
    return register(technique, project, subject, session, scan, resource,
                    bolus_arrival_index, mask, *in_files, **opts)


def register(technique, project, subject, session, scan, resource,
             bolus_arrival_index, mask, *in_files, **opts):
    """
    Runs the registration workflow on the given session scan images.
    
    :param technique: the registration technique
    :param project: the project name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param resource: the registration resource name
    :param bolus_arrival_index: the bolus uptake volume index
    :param mask: the required scan mask file
    :param in_files: the input session scan 3D NIfTI images
    :param opts: the :meth:`qipipe.pipeline.registration.run` keyword
        options
    :return: the realigned image file path array
    """
    # Note: There is always a mask and resource argument. The mask
    # file and resource name are either specified as an input or
    # built by the workflow. The mask and resource is optional in
    # the registration run function. Therefore, the registration
    # run keyword options include the mask and resource, as well
    # as any other options passed from the Function node.
    if not mask:
        raise ArgumentError("The register method is missing the mask")
    if not resource:
        raise ArgumentError("The register method is missing the"
                            " XNAT registration resource name")
    reg_opts = dict(mask=mask, resource=resource)
    reg_opts.update(opts)
    
    # The input scan files sorted by volume number.
    volumes = sorted(in_files, key=_extract_volume_number)
    # The files up to and including bolus arrival are not realigned.
    start = bolus_arrival_index + 1
    unrealigned = volumes[:start]
    # The bolus arrival is the initial fixed image.
    ref_0 = volumes[bolus_arrival_index]
    # The files to realign.
    reg_inputs = volumes[start:]
    
    # Register the files.
    realigned = registration.run(technique, project, subject, session, scan,
                                 ref_0, *reg_inputs, **reg_opts)
    # Upload the unrealigned images into the XNAT registration
    # resource.
    upload_unreg = XNATUpload(project=project, subject=subject,
                              session=session, scan=scan,
                              resource=resource, in_files=unrealigned,
                              modality='MR')
    upload_unreg.run()
    
    # Return the unregistered and registered result.
    return unrealigned + realigned


def _extract_volume_number(location):
    _, fname = os.path.split(location)
    match = VOLUME_FILE_PAT.match(fname)
    if not match:
        raise ArgumentError("The volume file name without directory does"
                            " not begin with volume<number>: %s" % fname)
    return int(match.group(1))


def roi(project, subject, session, scan, time_series, in_rois, opts):
    """
    Runs the ROI workflow on the given session scan images.
    
    :Note: see the :meth:`register` note.
    
    :param project: the project name
    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param time_series: the scan 4D time series
    :param in_rois: the :meth:`qipipe.pipeline.roi.run` input tuples
    :param opts: the :meth:`qipipe.pipeline.roi.run` keyword options
    """
    from qipipe.pipeline import roi
    
    roi.run(project, subject, session, scan, time_series, *in_rois, **opts)
