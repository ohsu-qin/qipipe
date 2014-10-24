import os
import re
import tempfile
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function, Merge)
from nipype.interfaces.dcmstack import MergeNifti
import qipipe
from . import staging
from .workflow_base import WorkflowBase
from .staging import StagingWorkflow
from .mask import MaskWorkflow
import registration
from ..interfaces import (XNATDownload, XNATUpload)
from qiutil import xnat_helper
from qiutil.logging_helper import logger
from ..staging.staging_helper import iter_stage

SCAN_TS_RSC = 'scan_ts'
"""The XNAT scan time series resouce name."""

MASK_RSC = 'mask'
"""The XNAT mask resouce name."""


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
    and runs it on the given inputs.
    
    If the *actions* option includes ``stage``, then the input is
    the :meth:`QIPipelineWorkflow.run_with_dicom_input` DICOM
    directories input. Otherwise, the input is the
    :meth:`QIPipelineWorkflow.run_with_scan_download` XNAT session
    labels input.

    :param inputs: the DICOM directories or XNAT session labels to
        process
    :param opts: the :class:`QIPipelineWorkflow` initializer options,
        as well as the following keyword options:
    :keyword collection: the AIRC collection name
    :keyword dest: the target staging parent directory
    :keyword scan_type: the ``dce`` or ``t2`` scan type
        (default ``dce``)
    """
    # Tailor the actions.
    actions = opts.get('actions', _default_actions(**opts))
    opts['actions'] = actions
    if 'stage' in actions:
        _run_with_dicom_input(*inputs, **opts)
    else:
        _run_with_xnat_input(*inputs, **opts)


def _default_actions(**opts):
    """
    Returns the default actions from the given options, as follows:

    * The default actions always include ``model``.

    * If the ``registration`` resource option is not set, then ``stage``
      is included in the actions.

    :param opts: the :meth:`run` options
    :return: the default actions
    """
    if 'registration' in opts:
        return ['register', 'model']
    else:
        return  ['stage', 'register', 'model']


def _run_with_dicom_input(*inputs, **opts):
    """
    :param inputs: the :meth:`QIPipelineWorkflow.run_with_dicom_input`
        inputs
    :param opts: the :meth:`run` options
    """
    collection = opts.pop('collection', None)
    dest = opts.get('dest', None)
    wf_gen = QIPipelineWorkflow(**opts)
    # Run the workflow on each session.
    for sbj, sess, ser_dicom_dict in iter_stage(collection, *inputs, **opts):
        wf_gen.run_with_dicom_input(collection, sbj, sess, ser_dicom_dict,
                                    dest)


def _run_with_xnat_input(*inputs, **opts):
    """
    Run the pipeline with a XNAT download.
    Each input is either a session label or path, e.g.
    ``Breast012_Session03`` or  ``Breast012/Session03``.
    
    :param inputs: the XNAT session labels or paths
    :param opts: the ``project`` and :class:`QIPipelineWorkflow`
        initializer options
    """
    prj = opts.pop('project', qipipe.project())
    with xnat_helper.connection() as xnat:
        for label in inputs:
            # Convert a path to a label.
            if '/' in label:
                label = label.sub('/', '_')
            sbj, sess = xnat_helper.parse_session_label(label)
            # Check for an existing mask.
            mask_obj = xnat.find(project=prj, subject=sbj, session=sess,
                                 resource=MASK_RSC)
            if mask_obj and mask_obj.files().get():
                opts['mask'] = MASK_RSC
                status = 'found'
            else:
                status = 'not found'
            logger(__name__).debug("The %s %s resource %s was %s." %
                                (sbj, sess, MASK_RSC, status))
            
            # If registration or modeling will be performed, then check
            # for an existing scan time series.
            if 'register' in opts['actions'] or 'model' in opts['actions']:
                scan_ts_obj = xnat.find(project=prj, subject=sbj, session=sess,
                                        resource=SCAN_TS_RSC)
                if scan_ts_obj and scan_ts_obj.files().get():
                    opts['scan_time_series'] = SCAN_TS_RSC
                    status = 'found'
                else:
                    status = 'not found'
                logger(__name__).debug("The %s %s scan time series resource %s was"
                                   " %s." % (sbj, sess, SCAN_TS_RSC, status))
            
            # If modeling will be performed on a specified registration
            # resource, then check for an existing realigned time series.
            if 'model' in opts['actions'] and 'registration' in opts:
                reg_ts_rsc = opts['registration'] + '_ts'
                reg_ts_obj = xnat.find(project=prj, subject=sbj, session=sess,
                             resource=reg_ts_rsc)
                if reg_ts_obj and reg_ts_obj.files().get():
                    opts['realigned_time_series'] = reg_ts_rsc
                    status = 'found'
                else:
                    status = 'not found'
                logger(__name__).debug("The %s %s realigned time series resource %s"
                                   " was %s." % (sbj, sess, reg_ts_rsc, status))

            # Execute the workflow.
            wf_gen = QIPipelineWorkflow(**opts)
            wf_gen.run_with_scan_download(xnat, prj, sbj, sess)


class ArgumentError(Exception):
    pass


class NotFoundError(Exception):
    pass


class QIPipelineWorkflow(WorkflowBase):
    """
    QIPipeline builds and executes the OHSU QIN workflows.
    The pipeline builds a composite workflow which stitches together
    the following constituent workflows:

    - staging: Prepare the new AIRC DICOM visits, as described in
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
      the previously staged series scan stack images are downloaded.

    - If registration is performed and the ``registration`` resource option
      is set, then the previously realigned images with the given resource
      name are downloaded. The remaining scans are registered.

    - If registration or modeling is performed and the XNAT ``mask``
      resource is found, then that resource file is downloaded. Otherwise,
      the mask is created from the staged images.

    The QIN workflow input node is *input_spec* with the following
    fields:

    - *subject*: the subject name

    - *session*: the session name

    In addition, if the staging or registration workflow is enabled
    then the *iter_series* node iterables input includes the
    following fields:

    - *series*: the scan number

    - *dest*: the target staging directory, if the staging
      workflow is enabled

    The constituent workflows are combined as follows:

    - The staging workflow input is the QIN workflow input.

    - The mask and reference workflow input images is the newly or
      previously staged scan NiFTI image files.

    - The modeling workflow input images is the combination of previously
      and newly realigned image files.

    The pipeline execution workflow is also available as the *workflow*
    instance variable. The workflow input node is named *input_spec*
    with the same input fields as the
    :class:`qipipe.staging.RegistrationWorkflow` workflow *input_spec*.

    .. _AIRC Grid Engine: https://everett.ohsu.edu/wiki/GridEngine
    """

    REG_SERIES_PAT = re.compile('series(\d+)_reg_')

    def __init__(self, **opts):
        """
        :param opts: the following initialization options:
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword cfg_file: the configuration file path containing
            class:`qipipe.pipeline.registration.WorkflowBase'
            configuration overrides
        :keyword dry_run: flag indicating whether to prepare but not
            run the pipeline
        :keyword mask: the XNAT mask reconstruction name
        :keyword registration: the XNAT registration reconstruction name
        :keyword technique: the
            class:`qipipe.pipeline.registration.RegistrationWorkflow`
            technique
        """
        # The dry run and configuration file options are processed by methods
        # defined in QIPipelineWorkflow. The remaining options are processed
        # by methods defined in the superclass WorkflowBase.
        base_opts = {}
        for opt in ['cfg_file', 'dry_run']:
            if opt in opts:
                base_opts[opt] = opts.pop(opt)
        super(QIPipelineWorkflow, self).__init__(logger(__name__), **base_opts)

        self.registration_resource = None
        """The registration XNAT reconstruction name."""

        self.modeling_resource = None
        """The modeling XNAT resource name."""

        self.workflow = self._create_workflow(**opts)
        """
        The pipeline execution workflow. The execution workflow is executed by
        calling the :meth:`run_with_dicom_input` or
        :meth:`run_with_scan_download` method.
        """

    def run_with_dicom_input(self, collection, subject, session,
                             ser_dicom_dict, **opts):
        """
        :param collection: the AIRC collection name
        :param subject: the subject name
        :param session: the session name
        :param inputs: the input AIRC visit directories
        :param opts: the :meth:`qipipeline.staging.staging.set_workflow_inputs`
            options
        """
        staging.set_workflow_inputs(self.workflow, collection, subject,
                                    session, ser_dicom_dict, **opts)
        self._run_workflow(self.workflow)

    def run_with_scan_download(self, xnat, project, subject, session):
        """
        Runs the execution workflow on downloaded scan image files.
        """
        self._logger.debug("Processing the %s %s %s scans..." %
                           (project, subject, session))

        # Get the scan numbers.
        scans = xnat.get_scan_numbers(project, subject, session)
        if not scans:
            raise NotFoundError("The QIN pipeline did not find a %s %s %s"
                                " scan." % (project, subject, session))

        # Partition the input scans into those which are already
        # registered and those which need to be registered.
        reg_scans, unreg_scans = self._partition_scans(xnat, project, subject,
                                                       session, scans)

        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.unregistered_series = unreg_scans

        # Execute the workflow.
        self._run_workflow(self.workflow)
        self._logger.debug("Processed %d %s %s %s scans." %
                           (len(scans), project, subject, session))

    def _partition_scans(self, xnat, project, subject, session, scans):
        """
        Partitions the given scans into those which have a corresponding
        registration reconstruction file and those which don't.

        :return: the (registered, unregistered) scan numbers
        """
        # The XNAT registration object.
        if self.registration_resource:
            reg_obj = xnat.get_experiment_resource(project, subject, session,
                                                   self.registration_resource)
        else:
            reg_obj = None
        # If the registration has not yet been performed, then
        # download all of the scans.
        if not (reg_obj and reg_obj.exists()):
            return [], scans

        # The realigned scan numbers.
        reg_scans = set(self._registered_scans(reg_obj))
        
        # The unregistered scan numbers.
        unreg_scans = set(scans) - reg_scans
        self._logger.debug("The %s %s %s resource has %d registered scans"
                           " and %d unregistered scans." %
                           (project, subject, session, len(reg_scans),
                            len(unreg_scans)))

        return (reg_scans, unreg_scans)

    def _registered_scans(self, reg_obj):
        """
        Returns the scans which have a corresponding registration
        reconstruction file.

        :param reg_obj: the XNAT registration reconstruction object
        :return: the registered scan numbers
        """
        # The XNAT registration file names.
        reg_files = reg_obj.files().get()
        # Match on the realigned scan file pattern.
        matches = ((QIPipelineWorkflow.REG_SERIES_PAT.match(f)
                    for f in reg_files))

        return [int(match.group(1)) for match in matches if match]

    def _create_workflow(self, **opts):
        """
        Builds the reusable pipeline workflow described in
        :class:`qipipe.pipeline.qipipeline.QIPipeline`.

        :param opts: the constituent workflow initializer options
        :return: the Nipype workflow
        """
        self._logger.debug("Building the QIN pipeline execution workflow...")

        # This is a long method body with the following stages:
        #
        # 1. Gather the options.
        # 2. Create the constituent workflows.
        # 3. Tie together the constituent workflows.
        #
        # The constituent workflows are created in back-to-front order,
        # i.e. modeling, registration, reference, mask, staging.
        # This order makes it easier to determine whether to create
        # an upstream workflow depending on the presence of downstream
        # workflows, e.g. the mask is not created if registration
        # is not performed.
        #
        # By contrast, the workflows are tied together in front-to-back
        # order.

        # The work directory used for the master workflow and all
        # constituent workflows.
        if 'base_dir' in opts:
            base_dir = os.path.abspath(opts['base_dir'])
        else:
            base_dir = tempfile.mkdtemp()

        # The execution workflow.
        exec_wf = pe.Workflow(name='qipipeline', base_dir=base_dir)

        # The workflow options.
        actions = opts.get('actions', ['stage', 'register', 'model'])
        mask_rsc = opts.get('mask')
        scan_ts_rsc = opts.get('scan_time_series')
        reg_rsc = opts.get('registration')
        reg_ts_rsc = opts.get('realigned_time_series')
        reg_technique = opts.get('technique')

        # Set the project, if necessary.
        if 'project' in opts:
            qipipe.project(opts['project'])
            self._logger.debug("Set the XNAT project to %s." %
                               qipipe.project())
        
        # Set the registration resource instance variable.
        if reg_rsc:
            self.registration_resource = reg_rsc

        # The modeling workflow. Since the proprietary fastfit module might
        # not be available, only import the ModelingWorkflow on demand if
        # modeling is required.
        if 'model' in actions:
            from .modeling import ModelingWorkflow
            mdl_wf_gen = ModelingWorkflow(base_dir=base_dir)
            mdl_wf = mdl_wf_gen.workflow
            self.modeling_resource = mdl_wf_gen.resource
        else:
            mdl_wf = None

        # The registration workflow node.
        if 'register' in actions:
            reg_inputs = ['subject', 'session', 'in_files',
                          'bolus_arrival_index', 'mask', 'opts']
            
            # The registration function keyword options.
            reg_opts = dict(base_dir=base_dir)
            if reg_technique:
                reg_opts['technique'] = reg_technique
            # If the resource was not specified, then make a new
            # resource name.
            if not reg_rsc:
                new_reg_rsc = registration.generate_resource_name()
                self.registration_resource = new_reg_rsc
            # Add the resource name to the registration options.
            reg_opts['resource'] = self.registration_resource
            
            # The registration function.
            reg_xfc = Function(input_names=reg_inputs,
                               output_names=['out_files'],
                               function=register_scans)
            reg_node = pe.Node(reg_xfc, name='registration')
            reg_node.inputs.opts = reg_opts
        else:
            self._logger.info("Skipping registration.")
            reg_node = None

        # Registration and modeling require a mask.
        if (reg_node or mdl_wf) and not mask_rsc:
            mask_wf = MaskWorkflow(base_dir=base_dir).workflow
        else:
            self._logger.info("Skipping mask creation.")
            mask_wf = None

        # The staging workflow.
        if 'stage' in actions:
            stg_wf = StagingWorkflow(base_dir=base_dir).workflow
        else:
            self._logger.info("Skipping staging.")
            stg_wf = None

        # Validate that there is at least one constituent workflow.
        if not any([stg_wf, reg_node, mdl_wf]):
            raise ArgumentError("No workflow was enabled.")

        # The workflow input fields.
        input_fields = ['subject', 'session']
        iter_series_fields = ['series']
        # The staging workflow has additional input fields.
        # Partial registration requires the unregistered scans input.
        if stg_wf:
            input_fields.append('collection')
            iter_series_fields.append('dest')
        elif reg_node and reg_rsc:
            input_fields.append('unregistered_series')
        
        # The workflow input node.
        input_spec_xfc = IdentityInterface(fields=input_fields)
        input_spec = pe.Node(input_spec_xfc, name='input_spec')
        # Most workflows require a series iterator node.
        if stg_wf or reg_node or mask_wf or (mdl_wf and not scan_ts_rsc):
            iter_series_xfc = IdentityInterface(fields=iter_series_fields)
            iter_series = pe.Node(iter_series_xfc, name='iter_series')
        # Staging requires a DICOM iterator node.
        if stg_wf:
            iter_dicom_xfc = IdentityInterface(fields=['series', 'dicom_file'])
            iter_dicom = pe.Node(iter_dicom_xfc, name='iter_dicom')
            exec_wf.connect(iter_series, 'series', iter_dicom, 'series')

        # Stitch together the workflows:

        # If staging is enabled, then stage the DICOM input.
        # Otherwise, if the registration, mask or reference
        # workflow is enabled, then download the scan files.
        if stg_wf:
            for field in input_spec.inputs.copyable_trait_names():
                exec_wf.connect(input_spec, field,
                                stg_wf, 'input_spec.' + field)
            for field in iter_series.inputs.copyable_trait_names():
                exec_wf.connect(iter_series, field,
                                stg_wf, 'iter_series.' + field)
            exec_wf.connect(iter_dicom, 'dicom_file',
                            stg_wf, 'iter_dicom.dicom_file')

        # Some workflows require the scans. If staging is
        # enabled then collect the staged NiFTI scan images. Otherwise,
        # download the XNAT NiFTI scan images. In either case, the
        # staged images are collected in a node named 'staged' with
        # output 'images'.
        if reg_node or mask_wf or (mdl_wf and not scan_ts_rsc):
            if stg_wf:
                staged = pe.JoinNode(IdentityInterface(fields=['out_files']),
                                    joinsource='iter_series', name='staged')
                exec_wf.connect(stg_wf, 'output_spec.image',
                                staged, 'out_files')
            else:
                dl_scans_xfc = XNATDownload(project=qipipe.project(),
                                            container_type='scan',
                                            resource='NIFTI')
                staged = pe.Node(dl_scans_xfc, name='staged')
                exec_wf.connect(input_spec, 'subject', staged, 'subject')
                exec_wf.connect(input_spec, 'session', staged, 'session')

        # Registration and modeling require a time series, mask and
        # bolus arrival.
        if reg_node or mdl_wf:
            # If a time series resource name was specified, then download
            # the time series. Otherwise, make the time series.
            if scan_ts_rsc:
                dl_scan_ts_xfc = XNATDownload(project=qipipe.project(),
                                              resource=scan_ts_rsc)
                scan_ts = pe.Node(dl_scan_ts_xfc,
                                  name='download_scan_time_series')
                exec_wf.connect(input_spec, 'subject', scan_ts, 'subject')
                exec_wf.connect(input_spec, 'session', scan_ts, 'session')
            else:
                # Merge the staged files.
                scan_ts = pe.Node(MergeNifti(out_format=SCAN_TS_RSC),
                                  name='merge_scans')
                exec_wf.connect(staged, 'out_files', scan_ts, 'in_files')
                # Upload the time series.
                ul_scan_ts_xfc = XNATUpload(project=qipipe.project(),
                                            resource=SCAN_TS_RSC)
                ul_scan_ts = pe.Node(ul_scan_ts_xfc,
                                     name='upload_scan_time_series')
                exec_wf.connect(input_spec, 'subject', ul_scan_ts, 'subject')
                exec_wf.connect(input_spec, 'session', ul_scan_ts, 'session')
                exec_wf.connect(scan_ts, 'out_file', ul_scan_ts, 'in_files')

            # If a mask resource name was specified, then download the mask.
            # Otherwise, make the mask.
            if mask_rsc:
                dl_mask_xfc = XNATDownload(project=qipipe.project(),
                                           resource=mask_rsc)
                download_mask = pe.Node(dl_mask_xfc, name='download_mask')
                exec_wf.connect(input_spec, 'subject', download_mask, 'subject')
                exec_wf.connect(input_spec, 'session', download_mask, 'session')
            else:
                assert mask_wf, "The mask workflow is missing"
                exec_wf.connect(input_spec, 'subject',
                                mask_wf, 'input_spec.subject')
                exec_wf.connect(input_spec, 'session',
                                mask_wf, 'input_spec.session')
                exec_wf.connect(scan_ts, 'out_file',
                                mask_wf, 'input_spec.time_series')
            
            # Compute the bolus arrival from the scan time series.
            bolus_arv_xfc = Function(input_names=['time_series'],
                                     output_names=['bolus_arrival_index'],
                                     function=bolus_arrival_index_or_zero)
            bolus_arv = pe.Node(bolus_arv_xfc, name='bolus_arrival_index')
            exec_wf.connect(scan_ts, 'out_file', bolus_arv, 'time_series')

        # If registration is enabled, then register the staged images.
        if reg_node:
            exec_wf.connect(input_spec, 'subject', reg_node, 'subject')
            exec_wf.connect(input_spec, 'session', reg_node, 'session')
            # The staged input.
            if stg_wf or not reg_rsc:
                exec_wf.connect(staged, 'out_files', reg_node, 'in_files')
            else:
                # Select only the unregistered scans.
                sel_unreg_xfc = Function(input_names=['scans', 'in_files'],
                                         output_names=['out_files'],
                                         function=select_scan_files)
                unreged = pe.Node(sel_unreg_xfc, name='unregistered')
                exec_wf.connect(input_spec, 'unregistered_series',
                                unreged, 'scans')
                exec_wf.connect(staged, 'out_files', unreged, 'in_files')
                exec_wf.connect(unreged, 'out_files', reg_node, 'in_files')
            # The mask input.
            if mask_wf:
                exec_wf.connect(mask_wf, 'output_spec.mask', reg_node, 'mask')
            else:
                exec_wf.connect(download_mask, 'out_file', reg_node, 'mask')
            # The bolus arrival.
            exec_wf.connect(bolus_arv, 'bolus_arrival_index',
                            reg_node, 'bolus_arrival_index')

        # If the modeling workflow is enabled, then model the scan or realigned
        # images.
        if mdl_wf:
            exec_wf.connect(input_spec, 'subject',
                            mdl_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session',
                            mdl_wf, 'input_spec.session')
            # The mask input.
            if mask_wf:
                exec_wf.connect(mask_wf, 'output_spec.mask',
                                mdl_wf, 'input_spec.mask')
            else:
                exec_wf.connect(download_mask, 'out_file',
                                mdl_wf, 'input_spec.mask')
            # The bolus arrival.
            exec_wf.connect(bolus_arv, 'bolus_arrival_index',
                            mdl_wf, 'input_spec.bolus_arrival_index')

            # If registration is enabled, then the 4D time series
            # is created by that workflow, otherwise download the
            # previously created 4D time series.
            if reg_ts_rsc:
                # Download the XNAT time series file.
                ts_dl_xfc = XNATDownload(project=qipipe.project(),
                                         resource=reg_ts_rsc)
                reg_ts = pe.Node(ts_dl_xfc, name='download_reg_time_series')
                exec_wf.connect(input_spec, 'subject', reg_ts, 'subject')
                exec_wf.connect(input_spec, 'session', reg_ts, 'session')
                exec_wf.connect(reg_ts, 'out_file',
                                mdl_wf, 'input_spec.time_series')
            elif self.registration_resource:
                # Merge the realigned images to 4D.
                reg_ts_rsc = self.registration_resource + '_ts'
                merge_reg = pe.Node(MergeNifti(out_format=reg_ts_rsc),
                                    name='merge_reg')
                
                # If the registration reconstruction name was specified,
                # then download the previously realigned images.
                if reg_rsc:
                    reg_dl_xfc = XNATDownload(project=qipipe.project(),
                                              resource=reg_rsc)
                    download_reg = pe.Node(reg_dl_xfc,
                                           name='download_realigned_images')
                    exec_wf.connect(input_spec, 'subject',
                                    download_reg, 'subject')
                    exec_wf.connect(input_spec, 'session',
                                    download_reg, 'session')
                    if reg_node:
                        # Merge the previously and newly realigned images.
                        concat_reg = pe.Node(Merge(2), name='concat_reg')
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
                        "The QIN pipeline cannot perform modeling on the"
                        " registration result, since the registration"
                        " workflow is disabled and no registration resource"
                        " was specified.")

                # Upload the realigned time series to XNAT.
                upload_reg_ts_xfc = XNATUpload(project=qipipe.project(),
                                               resource=reg_ts_rsc)
                upload_reg_ts = pe.Node(upload_reg_ts_xfc,
                                        name='upload_reg_time_series')
                exec_wf.connect(input_spec, 'subject',
                                upload_reg_ts, 'subject')
                exec_wf.connect(input_spec, 'session',
                                upload_reg_ts, 'session')
                exec_wf.connect(merge_reg, 'out_file',
                                upload_reg_ts, 'in_files')
                
                # Pass the realigned time series to modeling.
                exec_wf.connect(merge_reg, 'out_file',
                                mdl_wf, 'input_spec.time_series')
            else:
                # Model the scan input.
                exec_wf.connect(scan_ts, 'out_file',
                                mdl_wf, 'input_spec.time_series')

        # Set the configured workflow node inputs and plug-in options.
        self._configure_nodes(exec_wf)

        self._logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
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
            _, path = tempfile.mkstemp()
            try:
                opts = dict(base_dir=self.workflow.base_dir, dry_run=True)
                register_scans('Dummy', 'Dummy', [path], 0, path, opts)
            finally:
                os.remove(path)


def bolus_arrival_index_or_zero(time_series):
    from qipipe.helpers.bolus_arrival import (bolus_arrival_index,
                                              BolusArrivalError)

    # Determines the bolus uptake. If it could not be determined,
    # then the first series is taken to be the uptake.
    try:
        return bolus_arrival_index(time_series)
    except BolusArrivalError:
        return 0


def select_scan_files(scans, in_files):
    """
    :param scans: the scan numbers
    :param in_files: the scan files
    :return: the scan files for the given series 
    """
    import re
    
    scans = set(scans)
    series_pat = re.compile("series(\d{3}).nii.gz$")
    return [f for f in in_files
            if int(series_pat.search(f).group(1)) in scans]


def register_scans(subject, session, in_files, bolus_arrival_index,
                   mask, opts):
    """
    Runs the registration workflow on the given session scan images.

    :Note: contrary to Python convention, the opts method parameter
      is a required dictionary rather than a keyword aggregate (e.g.
      ``**opts``). The Nipype ``Function`` interface does not support
      method aggregates.

    :param subject: the subject name
    :param session: the session name
    :param in_files: the input session scan images
    :param mask: the image mask file path
    :param bolus_arrival_index: the bolus uptake series index
    :param opts: the :meth:`qipipe.pipeline.registration.run` keyword
        options
    :return: the realigned image file path array
    """
    from qipipe.pipeline import registration

    return registration.run(subject, session, in_files, bolus_arrival_index,
                            mask, **opts)
