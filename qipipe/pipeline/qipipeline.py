import os
import re
import tempfile
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.dcmstack import MergeNifti
from .. import project
from . import staging
from .workflow_base import WorkflowBase
from .staging import StagingWorkflow
from .mask import MaskWorkflow
from .reference import ReferenceWorkflow
from .registration import RegistrationWorkflow
from .modeling import ModelingWorkflow
from ..interfaces import (Gate, XNATDownload, XNATUpload)
from ..helpers import xnat_helper
from ..helpers.logging_helper import logger


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
    and runs it on the given inputs.

    :param inputs: the
        :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run` inputs
    :param opts: the :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
        initializer and
        :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run` options
    :return: the :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        result
    """
    # If the inputs are not directories, then disable staging.
    wf_gen = QIPipelineWorkflow(**opts)

    return wf_gen.run(*inputs, **opts)


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

    - reference: Create the fixed reference image from the staged images,
      as described in :class:`qipipe.pipeline.reference.ReferenceWorkflow`

    - registration: Mask, register and realign the staged images,
      as described in :class:`qipipe.pipeline.registration.RegistrationWorkflow`

    - modeling: Perform PK modeling as described in
      :class:`qipipe.pipeline.modeling.ModelingWorkflow`

    The pipeline workflow depends on the initialization options as follows:

    - If none of the *skip_staging*, *mask*, *reference* or *registration*
      options are set, then the DICOM files are staged for the subject
      directory inputs. Otherwise, staging is not performed. In that case,
      if registration is enabled as described below, then the previously
      staged series scan stack images are downloaded.

    - The scan images are registered if and only if the *skip_registration*
      option is not set to True. If the *registration* option is set,
      then the previously realigned images with the given reconstruction
      name are downloaded. The remaining scans are registered.

    - If registration is performed and the *mask* option is set, then the
      given mask XNAT reconstruction is downloaded. Otherwise, the mask is
      created from the staged images prior to registration.

    - If registration is performed and the *reference* option is set, then
      the given mask XNAT reconstruction is downloaded. Otherwise, the
      reference is created from the staged images prior to registration.

    - PK modeling is performed if and only if the *skip_modeling* option is
      not set to True.

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

    The easiest way to execute the pipeline is to call the
    :meth:`qipipe.pipeline.qipipeline.run` method.

    The pipeline execution workflow is also available as the *workflow*
    instance variable. The workflow input node is named *input_spec*
    with the same input fields as the
    :class:`qipipe.staging.RegistrationWorkflow` workflow *input_spec*.
    """

    REG_SERIES_PAT = re.compile('series(\d+)_reg_')

    def __init__(self, **opts):
        """
        :param opts: the following initialization options:
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword mask: the XNAT mask reconstruction name
        :keyword registration: the XNAT registration reconstruction name
        :keyword skip_registration: flag indicating whether to skip
            registration
        :keyword technique: the
            class:`qipipe.pipeline.registration.RegistrationWorkflow`
            technique
        :keyword skip_modeling: flag indicating whether to skip modeling
        """
        super(QIPipelineWorkflow, self).__init__(logger(__name__))

        self.registration_reconstruction = None
        """The registration XNAT reconstruction name."""

        self.modeling_assessor = None
        """The modeling XNAT assessor name."""

        self.workflow = self._create_workflow(**opts)
        """
        The pipeline execution workflow. The execution workflow is executed by
        calling the :meth:`qipipe.pipeline.modeling.QIPipelineWorkflow.run`
        method.
        """

    def run(self, *inputs, **opts):
        """
        Runs the OHSU QIN pipeline on the the given inputs, as follows:

        This method returns a *{subject: {session: results}}*  dictionary
        for the processed sessions, where results is a dictionary with
        the following items:

        - *registration*: the registration XNAT reconstruction name

        - *modeling*: the modeling XNAT assessor name

        If the :mod:`qipipe.pipeline.distributable' ``DISTRIBUTABLE`` flag
        is set, then the execution is distributed using the
        `AIRC Grid Engine`_.

        .. _AIRC Grid Engine: https://everett.ohsu.edu/wiki/GridEngine

        :param inputs: the AIRC subject directories or XNAT session
            names
        :param opts: the meth:`qipipe.pipeline.staging.run` options,
            augmented with the following keyword arguments:
        :keyword collection: the AIRC collection name defined by
            :mod:`qipipe.staging.airc_collection`, required if
            staging is enabled
        :return: the new *{subject: session: results}*  dictionary
        """
        # If staging is enabled, then start with the DICOM inputs.
        # Otherwise, if registration is enabled, then start with a
        # scan download. Otherwise, start with registration download.
        if self.workflow.get_node('staging.input_spec'):
            sbj_sess_dict = self._run_with_dicom_input(*inputs, **opts)
        elif self.workflow.get_node('registration.input_spec'):
            sbj_sess_dict = self._run_with_scan_download(*inputs, **opts)
        else:
            sbj_sess_dict = self._run_with_registration_download(*inputs)

        # Return the new {subject: {session: {results}}} dictionary,
        # where the results dictionary has keys registration and modeling.
        # Each session has the same unqualified XNAT registration
        # reconstruction and modeling assessor name. Therefore, make a
        # template containing these names and copy the template into the
        # {subject: {session: {results}}} output dictionary.
        template = {}
        if self.registration_reconstruction:
            template['registration'] = self.registration_reconstruction
        if self.modeling_assessor:
            template['modeling'] = self.modeling_assessor
        output_dict = defaultdict(lambda: defaultdict(dict))
        for sbj, sessions in sbj_sess_dict.iteritems():
            for sess in sessions:
                output_dict[sbj][sess] = template.copy()

        return output_dict

    def _run_with_dicom_input(self, *inputs, **opts):
        collection = opts.pop('collection', None)
        if not collection:
            raise ArgumentError('QIPipeline is missing the collection argument')
        opts['workflow'] = self.workflow
        opts['base_dir'] = self.workflow.base_dir
        stg_dict = staging.run(collection, *inputs, **opts)

        return {sbj: sess_dict.keys()
                for sbj, sess_dict in stg_dict.iteritems()}

    def _run_with_scan_download(self, *inputs, **opts):
        """
        Runs the execution workflow on downloaded scan image files.

        :param inputs: the XNAT session labels
        :param opts: the meth:`qipipe.pipeline.qipipeline._run_on_scans`
            keyword options:
        :keyword skip_registration: flag indicating whether to register
            the scan images
        :return: the the XNAT *{subject: [session]}* dictionary
        """
        self._logger.debug("Running the QIN pipeline execution workflow...")

        # The {subject: [session]} return value.
        result_dict = defaultdict(list)

        # Parse the XNAT hierarchy for the inputs.
        sess_specs = self._parse_session_labels(inputs)
        # Validate the project.
        for spec in sess_specs:
            prj, _, _ = spec
            if prj != project():
                raise ArgumentError("The project %s in the session label %s"
                                 "differs from the current project %s" %
                                 (prj, spec, project()))

        # Run the workflow on each session's downloaded scans.
        with xnat_helper.connection() as xnat:
            for prj, sbj, sess in sess_specs:
                # Run the workflow.
                self._run_on_scans(xnat, prj, sbj, sess, **opts)
                # Capture the result.
                result_dict[sbj].append(sess)

        self._logger.debug("Completed the QIN pipeline execution workflow.")

        return result_dict

    def _run_on_scans(self, xnat, project, subject, session, **opts):
        """
        Runs the execution workflow on downloaded scan image files.

        :param project: the project name
        :param subject: the subject name
        :param session: the session name
        :param opts: the following keyword options:
        :keyword skip_registration: flag indicating whether to register
            the scan images
        """
        self._logger.debug("Processing the %s %s %s scans..." %
                           (project, subject, session))

        # Get the scan numbers.
        scans = xnat.get_scans(project, subject, session)
        if not scans:
            raise NotFoundError("The QIN pipeline did not find a %s %s %s"
                          " scan." % (project, subject, session))

        # Set the workflow input.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session

        # Partition the input scans into those which are already
        # registered and those which need to be registered.
        reg_scans, unreg_scans = self._partition_scans(
            xnat, project, subject, session, scans)

        # If registration is enabled, then there is a series iterator
        # node. In that case, set the series iterables to the
        # unregistered scans.
        iter_series = self.workflow.get_node('iter_series')
        if iter_series:
            iter_series.iterables = ('series', unreg_scans)

        # If modeling is enabled, then there is a download registered
        # scans node. If that is the case and there are registered scans,
        # then download the realigned images.
        download_reg = self.workflow.get_node('download_reg')
        if download_reg:
            # If registration is disabled, then validate that all scans
            # are registered.
            if opts.get('skip_registration') and unreg_scans:
                raise NotFoundError(
                    "The following %s %s %s scans are not registered: %s" %
                    (project, subject, session, unreg_scans))
            # Download each scan file.
            download_reg.iterables = ('scan', reg_scans)

        # Execute the workflow.
        self._run_workflow(self.workflow)
        self._logger.debug("Processed %d %s %s %s scans." %
                           (len(scans), project, subject, session))

    def _partition_scans(self, xnat, project, subject, session, scans):
        """
        Partitions the given scans into those which have a corresponding
        registration reconstruction file and those which don't.

        :return: the [registered, unregistered] scan numbers
        """
        # The XNAT registration object.
        reg_obj = xnat.get_reconstruction(project, subject, session,
                                          self.registration_reconstruction)
        # If the registration has not yet been performed, then
        # all of the scans are downloaded.
        if not reg_obj.exists():
            return [], scans
        # The XNAT registration file names.
        reg_files = reg_obj.out_resources().fetchone().files().get()
        reg_scans = [int(QIPipelineWorkflow.REG_SERIES_PAT.match(f).group(1))
                     for f in reg_files]

        return reg_scans, list(set(scans) - set(reg_scans))

    def _run_with_registration_download(self, *inputs):
        """
        Runs the execution workflow on downloaded reconstruction image files.

        :param inputs: the XNAT reconstruction labels
        :return: the the XNAT *{subject: [session]}* dictionary
        """
        self._logger.debug("Running the QIN pipeline execution workflow...")

        result_dict = defaultdict(list)
        exec_wf = self.workflow
        input_spec = exec_wf.get_node('input_spec')

        for prj, sbj, sess in self._parse_session_labels(inputs):
            self._logger.debug("Processing the %s %s %s realigned images..." %
                               (prj, sbj, sess))

            # Set the project id.
            project(prj)
            # Set the workflow input.
            input_spec.inputs.subject = sbj
            input_spec.inputs.session = sess

            # Execute the workflow.
            self._run_workflow(exec_wf)
            # Capture the result.
            result_dict[sbj].append(sess)

        self._logger.debug("Completed the QIN pipeline execution workflow.")

        return result_dict

    def _parse_session_labels(self, labels):
        """
        Parse the given XNAT session labels.

        :param labels: the XNAT session labels to parse
        :return: the [*(project, subject, session)*] name tuples
        :raise ValueError: if a label is not in
            *project*``_``*subject*``_``*session* format
        """
        return [xnat_helper.parse_session_label(label) for label in labels]

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
        # is not enabled.
        #
        # By contrast, the workflows are tied together in
        # front-to-back order.

        # The work directory used for the master workflow and all
        # constituent workflows.
        base_dir_opt = opts.get('base_dir')
        if base_dir_opt:
            base_dir = os.path.abspath(base_dir_opt)
        else:
            base_dir = tempfile.mkdtemp()

        # The execution workflow.
        exec_wf = pe.Workflow(name='qin_exec', base_dir=base_dir)

        # The workflow options.
        mask_recon = opts.get('mask')
        ref_recon = opts.get('reference')
        reg_recon = opts.get('registration')
        reg_technique = opts.get('technique')
        skip_staging = opts.get('skip_staging')
        skip_registration = opts.get('skip_registration')
        skip_modeling = opts.get('skip_modeling')

        # Set the project, if necessary.
        if 'project' in opts:
            project(opts.pop('project'))
            self._logger.debug("Set the XNAT project to %s." % project())

        # The modeling workflow.
        if skip_modeling:
            self._logger.info("Skipping modeling.")
            mdl_wf = None
        elif skip_registration and not reg_recon:
            raise ArgumentError(
                "The QIN pipeline cannot perform modeling, since the"
                " registration workflow is disabled and no realigned"
                " images will be downloaded.")
        else:
            mdl_wf_gen = ModelingWorkflow(base_dir=base_dir)
            mdl_wf = mdl_wf_gen.workflow
            self.modeling_assessor = mdl_wf_gen.assessor

        # The registration workflow. A resubmission registers only those
        # scans which have not yet been registered.
        if skip_registration:
            self._logger.info("Skipping registration.")
            reg_wf = None
        else:
            reg_opts = dict(base_dir=base_dir)
            if reg_technique:
                reg_opts['technique'] = reg_technique
            if reg_recon:
                reg_opts['reconstruction'] = reg_recon
            reg_wf_gen = RegistrationWorkflow(**reg_opts)
            self.registration_reconstruction = reg_wf_gen.reconstruction
            reg_wf = reg_wf_gen.workflow

        # The mask workflow.
        # Unless either registration or modeling are enabled, then
        # there is no need to create a mask.
        if (reg_wf or mdl_wf) and not mask_recon:
            mask_wf = MaskWorkflow(base_dir=base_dir).workflow
        else:
            self._logger.info("Skipping mask creation.")
            mask_wf = None

        # The reference workflow.
        # Unless registration is enabled, then there is no need to
        # create a reference.
        if reg_wf and not ref_recon:
            ref_wf = ReferenceWorkflow(base_dir=base_dir).workflow
        else:
            self._logger.info("Skipping reference creation.")
            ref_wf = None

        # The staging workflow.
        if skip_staging or reg_recon or mask_recon or ref_recon:
            self._logger.info("Skipping staging.")
            stg_wf = None
        else:
            # Stage the input DICOM files.
            stg_wf = StagingWorkflow(base_dir=base_dir).workflow

        # Validate that there is at least one constituent workflow.
        if not any([stg_wf, reg_wf, mdl_wf]):
            raise ArgumentError("No workflow was enabled.")

        # The workflow input fields.
        input_fields = ['subject', 'session']
        iter_series_fields = ['series']
        # The staging workflow has additional input fields.
        if stg_wf:
            input_fields.append('collection')
            iter_series_fields.append('dest')
        # The workflow input node.
        input_spec_xfc = IdentityInterface(fields=input_fields)
        input_spec = pe.Node(input_spec_xfc, name='input_spec')
        # The staging and registration workflows require a series
        # iterator node.
        if stg_wf or reg_wf:
            iter_series_xfc = IdentityInterface(fields=iter_series_fields)
            iter_series = pe.Node(iter_series_xfc, name='iter_series')
        if stg_wf:
            iter_dicom_xfc = IdentityInterface(fields=['series', 'dicom_file'])
            iter_dicom = pe.Node(iter_dicom_xfc, name='iter_dicom')

        # Stitch together the workflows:

        # If staging is enabled. then stage the DICOM input.
        # Otherwise, if the registration, mask or reference
        # workflow is enabled, then download the scan files.
        if stg_wf:
            for field in input_spec.inputs.copyable_trait_names():
                exec_wf.connect(input_spec, field,
                                stg_wf, 'input_spec.' + field)
            for field in iter_series.inputs.copyable_trait_names():
                exec_wf.connect(iter_series, field,
                                stg_wf, 'iter_series.' + field)
            for field in iter_dicom.inputs.copyable_trait_names():
                exec_wf.connect(iter_dicom, field,
                                stg_wf, 'iter_dicom.' + field)
        elif reg_wf or mask_wf or ref_wf:
            dl_xfc = XNATDownload(project=project())
            download_scan = pe.Node(dl_xfc, name='download_scan')
            exec_wf.connect(input_spec, 'subject',
                            download_scan, 'subject')
            exec_wf.connect(input_spec, 'session',
                            download_scan, 'session')
            exec_wf.connect(iter_series, 'series',
                            download_scan, 'scan')

        # The mask and reference workflow inputs include the staged
        # images as a list.
        if mask_wf or ref_wf:
            staged = pe.JoinNode(IdentityInterface(fields=['images']),
                                 joinsource='iter_series', name='staged')
            if stg_wf:
                exec_wf.connect(stg_wf, 'output_spec.image', staged, 'images')
            else:
                exec_wf.connect(download_scan, 'out_file', staged, 'images')

        # If the mask workflow is enabled, then make the mask.
        # Otherwise, download the mask if necessary.
        if mask_wf:
            exec_wf.connect(input_spec, 'subject',
                            mask_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session',
                            mask_wf, 'input_spec.session')
            exec_wf.connect(staged, 'images',
                            mask_wf, 'input_spec.images')
        elif mask_recon and (reg_wf or mdl_wf):
            dl_xfc = XNATDownload(project=project(), reconstruction=mask_recon)
            download_mask = pe.Node(dl_xfc, name='download_mask')
            exec_wf.connect(input_spec, 'subject', download_mask, 'subject')
            exec_wf.connect(input_spec, 'session', download_mask, 'session')

        # If the reference workflow is enabled, then make the mask.
        # Otherwise, download the reference if necessary.
        if ref_wf:
            exec_wf.connect(input_spec, 'subject',
                            ref_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session',
                            ref_wf, 'input_spec.session')
            exec_wf.connect(staged, 'images',
                            ref_wf, 'input_spec.images')
        elif ref_recon and reg_wf:
            dl_xfc = XNATDownload(project=project(), reconstruction=ref_recon)
            download_ref = pe.Node(dl_xfc, name='download_ref')
            exec_wf.connect(input_spec, 'subject', download_ref, 'subject')
            exec_wf.connect(input_spec, 'session', download_ref, 'session')

        # The 4D time series file is created from the registration output
        # realigned images and used by modeling.
        ts_base = "%s_ts" % self.registration_reconstruction

        # If the reference workflow is enabled, then register the staged
        # images.
        if reg_wf:
            exec_wf.connect(input_spec, 'subject',
                            reg_wf, 'input_spec.subject')
            exec_wf.connect(input_spec, 'session',
                            reg_wf, 'input_spec.session')
            # The staged input.
            if stg_wf:
                exec_wf.connect(stg_wf, 'output_spec.image',
                                reg_wf, 'input_spec.image')
            else:
                exec_wf.connect(download_scan, 'out_file',
                                reg_wf, 'input_spec.image')
            # The mask input.
            if mask_wf:
                exec_wf.connect(mask_wf, 'output_spec.mask',
                                reg_wf, 'input_spec.mask')
            else:
                exec_wf.connect(download_mask, 'out_file',
                                reg_wf, 'input_spec.mask')
            # The reference input.
            if ref_wf:
                exec_wf.connect(ref_wf, 'output_spec.reference',
                                reg_wf, 'input_spec.reference')
            else:
                exec_wf.connect(download_ref, 'out_file',
                                reg_wf, 'input_spec.reference')
            
            # Collect the registration realigned images output.
            new_reg_xfc = IdentityInterface(fields=['images'])
            new_reg = pe.JoinNode(new_reg_xfc, joinsource='iter_series',
                                  name='new_reg')
            exec_wf.connect(reg_wf, 'output_spec.image', new_reg, 'images')
            
            # If the registration reconstruction name was specified,
            # then download previously registered realigned images.
            if reg_recon:
                reg_dl_xfc = XNATDownload(project=project(),
                                          reconstruction=reg_recon)
                download_reg = pe.Node(reg_dl_xfc, name='download_reg')
                exec_wf.connect(input_spec, 'subject',
                                download_reg, 'subject')
                exec_wf.connect(input_spec, 'session',
                                download_reg, 'session')
                downloaded_reg_xfc = IdentityInterface(fields=['images'])
                downloaded_reg = pe.JoinNode(downloaded_reg_xfc,
                                             joinsource=download_reg,
                                             name='downloaded_reg')
                exec_wf.connect(download_reg, 'out_file',
                                downloaded_reg, 'images')

                # Merge the previously and newly realigned images.
                concat_reg = pe.Node(Merge(2), name='concat_reg')
                exec_wf.connect(downloaded_reg, 'images', concat_reg, 'in1')
                exec_wf.connect(new_reg, 'images', concat_reg, 'in2')

            # Merge the realigned images to 4D.
            merge_reg = pe.Node(MergeNifti(out_format=ts_base),
                                name='merge_reg')
            if reg_recon:
                exec_wf.connect(concat_reg, 'out', merge_reg, 'in_files')
            else:
                exec_wf.connect(new_reg, 'images', merge_reg, 'in_files')

            # Upload the realigned time series to XNAT.
            upload_reg_ts_xfc = XNATUpload(project=project(),
                reconstruction=self.registration_reconstruction,
                format='NIFTI')
            upload_reg_ts = pe.Node(upload_reg_ts_xfc, name='upload_reg_ts')
            exec_wf.connect(input_spec, 'subject', upload_reg_ts, 'subject')
            exec_wf.connect(input_spec, 'session', upload_reg_ts, 'session')
            exec_wf.connect(merge_reg, 'out_file', upload_reg_ts, 'in_files')

        # If the modeling workflow is enabled, then model the realigned
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
                exec_wf.connect(download_mask, 'output_file',
                                mdl_wf, 'input_spec.mask')

            # If registration is enabled, then the 4D time series
            # is created by that workflow, otherwise download the
            # previously created 4D time series.
            if reg_wf:
                exec_wf.connect(merge_reg, 'out_file',
                                mdl_wf, 'input_spec.time_series')
            else:
                # The 4D time series XNAT file name.
                ts_fname = ts_base + '.nii.gz'
                # Download the XNAT time series file.
                ts_dl_xfc = XNATDownload(project=project(),
                                         reconstruction=reg_recon,
                                         file=ts_fname)
                download_ts = pe.Node(ts_dl_xfc, name='download_ts')
                exec_wf.connect(input_spec, 'subject',
                                download_ts, 'subject')
                exec_wf.connect(input_spec, 'session',
                                download_ts, 'session')
                exec_wf.connect(download_ts, 'out_file',
                                mdl_wf, 'input_spec.time_series')

        # Set the configured workflow node inputs and plug-in options.
        self._configure_nodes(exec_wf)

        self._logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self._depict_workflow(exec_wf)

        return exec_wf
