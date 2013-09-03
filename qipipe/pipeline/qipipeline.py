import os
import tempfile
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from . import staging
from .workflow_base import WorkflowBase
from .staging import StagingWorkflow
from .mask import MaskWorkflow
from .registration import RegistrationWorkflow
from .modeling import ModelingWorkflow
from ..interfaces import XNATDownload
from ..helpers import xnat_helper
from ..helpers.project import project
from ..helpers.logging_helper import logger


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
    and runs it on the given inputs.

    :param inputs: the :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        inputs
    :param opts: the :class:`qipipe.pipeline.qipipeline.QIPipelineWorkflow`
        initializer and :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        options
    :return: the :meth:`qipipe.pipeline.qipipeline.QIPipelineWorkflow.run`
        result
    """
    # If the inputs are not directories, then disable staging.
    wf_gen = QIPipelineWorkflow(**opts)
    # The base directory is used solely for workflow creation.
    opts.pop('base_dir', None)
    return wf_gen.run(*inputs, **opts)


class QIPipelineWorkflow(WorkflowBase):

    """
    QIPipeline builds and executes the OHSU QIN workflows.
    The pipeline builds a composite workflow which stitches together
    the following constituent workflows:

    - staging: Prepare the new AIRC DICOM visits, as described in
      :class:`qipipe.staging.StagingWorkflow`

    - mask: Create a mask from the staged images,
      as described in :class:`qipipe.staging.MaskWorkflow`

    - registration: Mask, register and realign the staged images,
      as described in :class:`qipipe.staging.RegistrationWorkflow`

    - modeling: Perform PK modeling as described in
      :class:`qipipe.staging.ModelingWorkflow`

    The easiest way to execute the pipeline is to call the
    :meth:`qipipe.pipeline.qipipeline.run` method.

    The pipeline execution workflow is also available as the ``workflow``
    instance variable. The workflow input node is named ``input_spec``
    with the same input fields as the
    :class:`qipipe.staging.RegistrationWorkflow` workflow ``input_spec``.
    """

    def __init__(self, **opts):
        """
        The default workflow settings can be overriden by a configuration
        file specified in the *staging*, *mask*, *registration* or
        *modeling* option. If the *mask* option is set to False, then
        only staging is performed. If the *registration* option is set
        to false, then  registration is skipped and modeling is performed on
        the staged scans. If the *modeling* option is set to False, then
        PK modeling is not performed.

        :param opts: the following initialization options:
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword mask: the optional XNAT mask reconstruction name
        :keyword registration: the optional XNAT registration reconstruction
            name
        :keyword modeling: False to skip modeling
        """
        super(QIPipelineWorkflow, self).__init__(logger(__name__))

        self.workflow = self._create_workflow(**opts)
        """
        The pipeline execution workflow. The execution workflow is executed by
        calling the :meth:`qipipe.pipeline.modeling.QIPipelineWorkflow.run`
        method.
        """

        self.registration_reconstruction = None
        """The registration XNAT reconstruction name."""

        self.modeling_assessor = None
        """The modeling XNAT assessor name."""

    def run(self, *inputs, **opts):
        """
        Runs the OHSU QIN pipeline on the the given inputs, as follows:

        - If the *registration* option is set to a XNAT reconstruction
          name and the *resubmit* option is not set to True, then the
          realigned images are downloaded for the given XNAT session
          label inputs.

        - Otherwise, if the *staging* option is set to False, then the
          series scan stack images are downloaded for the given
          XNAT session label inputs.

        - Otherwise, the DICOM files are staged for the given subject
          directory inputs.

        The supported AIRC collections are listed in
        :mod:`qipipe.staging.airc_collection`.

        This method returns a *{subject: {session: results}}*  dictionary
        for the processed sessions, where results is a dictionary with
        the following items:

        - ``registration``: the registration XNAT reconstruction name

        - ``modeling``: the modeling XNAT assessor name

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
        # If registration is skipped, then start with the registration
        # download.
        # Otherwise, if staging is disabled, then run the download workflow.
        # Otherwise, delegate to staging with the execution workflow.
        if opts.get('registration') and 'resubmit' not in opts:
            sbj_sess_dict = self._run_with_registration_download(*inputs)
        elif opts.get('staging') == False:
            sbj_sess_dict = self._run_with_scan_download(*inputs)
        else:
            exec_wf = self.workflow
            if 'collection' not in opts:
                raise ValueError(
                    'QIPipeline is missing the collection argument')
            collection = opts.pop('collection')
            stg_dict = staging.run(collection, *inputs,
                                   base_dir=exec_wf.base_dir,
                                   workflow=exec_wf, **opts)
            sbj_sess_dict = {sbj: sess_dict.keys()
                             for sbj, sess_dict in stg_dict.iteritems()}

        # Return the new {subject: {session: {results}}} dictionary.
        # Each session has the same unqualified XNAT registration
        # reconstruction and modeling assessor name. Therefore, make
        # a template containing these names and copy the template
        # into the {subject: {session: {results}}} output dictionary.
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

    def _run_with_scan_download(self, *inputs):
        """
        Runs the execution workflow on downloaded scan image files.

        :param inputs: the XNAT session labels
        :return: the the XNAT *{subject: [session]}* dictionary
        """
        self._logger.debug("Running the QIN pipeline execution workflow...")
        result_dict = defaultdict(list)
        exec_wf = self.workflow
        input_spec = exec_wf.get_node('input_spec')

        with xnat_helper.connection() as xnat:
            for prj, sbj, sess in self._parse_session_labels(inputs):
                self._logger.debug(
                    "Processing the %s %s %s realigned images..." %
                    (prj, sbj, sess))

                # Set the project id.
                project(prj)
                # Get the scan numbers.
                scans = xnat.get_scans(prj, sbj, sess)
                if not scans:
                    raise IOError("The QIN pipeline did not find a %s %s %s"
                                  " scan." % (prj, sbj, sess))
                # Set the workflow input.
                input_spec.inputs.subject = sbj
                input_spec.inputs.session = sess
                iter_scan = exec_wf.get_node('staging.iter_scan')
                iter_scan.iterables = ('scan', scans)

                # Execute the workflow.
                self._run_workflow(exec_wf)
                # Capture the result.
                result_dict[sbj].append(sess)

        self._logger.debug("Completed the QIN pipeline execution workflow.")
        return result_dict

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
        A generator for *(project, subject, session)* names
        parsed from the given XNAT session labels.

        :param labels: the XNAT session labels to parse
        :yield: the *(project, subject, session)* name tuple
        :raise ValueError: if the label is not in
            *project*``_``*subject*``_``*session* format
        """
        for label in labels:
            # The project/subject/session name hierarchy
            names = xnat_helper.parse_canonical_label(label)
            if len(names) != 3:
                raise ValueError("The XNAT session label is not in the format"
                                 " project_subject_session: %s" % label)
            yield names

    def _create_workflow(self, **opts):
        """
        Builds the reusable pipeline workflow described in
        :class:`qipipe.pipeline.qipipeline.QIPipeline`.

        :param opts: the constituent workflow initializer options
        :return: the Nipype workflow
        """
        self._logger.debug("Building the QIN pipeline execution workflow...")

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
        resubmit = opts.get('resubmit')
        stg_opt = opts.get('staging')
        mask_opt = opts.get('mask')
        reg_opt = opts.get('registration')
        mdl_opt = opts.get('modeling')

        # The modeling workflow.
        if mdl_opt == False:
            self._logger.info("Skipping modeling.")
            mdl_wf = None
        elif reg_opt == False:
            raise ValueError(
                "The QIN pipeline cannot perform modeling, since the"
                " registration workflow is disabled and no realigned"
                " images will be downloaded.")
        else:
            mdl_wf_gen = ModelingWorkflow(base_dir=base_dir)
            mdl_wf = mdl_wf_gen.workflow
            self.modeling_assessor = mdl_wf_gen.assessor

        # The registration workflow.
        # If modeling is disabled, then there is no need to download
        # realigned images.
        if reg_opt:
            if resubmit:
                self._logger.debug("The QIN pipeline workflow will resubmit the"
                             " %s registration workflow." % reg_opt)
                reg_wf_gen = RegistrationWorkflow(
                    base_dir=base_dir, reconstruction=reg_opt)
                reg_wf = reg_wf_gen.workflow
                self.registration_reconstruction = reg_opt
            elif mdl_wf:
                # Download the input XNAT session reconstruction.
                reg_wf = self._create_registration_download_workflow(
                    base_dir=base_dir, recon=reg_opt)
                self.registration_reconstruction = reg_opt
                self._logger.debug("The QIN pipeline workflow will download the"
                             " registration reconstruction %s." % reg_opt)
            else:
                self._logger.info("Skipping %s realigned image download, since"
                                 " modeling is disabled." % reg_opt)
                reg_wf = None
        elif reg_opt == None:
            # The registration option is neither an existing XNAT
            # reconstruction nor False, so run the registration to
            # generate a new XNAT reconstruction.
            reg_wf_gen = RegistrationWorkflow(base_dir=base_dir)
            reg_wf = reg_wf_gen.workflow
            self.registration_reconstruction = reg_wf_gen.reconstruction
        else:
            # The registration option is False, so skip it.
            reg_wf = None

        # The mask workflow.
        # Unless both registration and modeling are enabled, then
        # there is no need to download a mask.
        if reg_wf or mdl_wf:
            if mask_opt:
                # Download the input XNAT session mask.
                mask_wf = self._create_mask_download_workflow(
                    base_dir=base_dir, recon=mask_opt)
                self._logger.debug("The QIN pipeline workflow will download the"
                             " mask reconstruction %s." % mask_opt)
            else:
                mask_wf = MaskWorkflow(base_dir=base_dir).workflow
        else:
             self._logger.info("Skipping mask download, since both"
                              " registration and modeling are disabled.")
             mask_wf = None

        # The staging workflow. If the mask and registration
        # XNAT objects are specified, then there is no need
        # to use the scan image files.
        if mask_opt and reg_opt:
            self._logger.info("Skipping staging, since the realigned images"
                             " and mask will be downloaded.")
            stg_wf = None
        elif stg_opt == False:
            # Download the input XNAT session images.
            stg_wf = self._create_scan_download_workflow(base_dir=base_dir)
        else:
            # Stage the input DICOM files
            stg_wf = StagingWorkflow(base_dir=base_dir).workflow

        # Validate that there is at least one constituent workflow.
        if not any([stg_wf, mask_wf, reg_wf, mdl_wf]):
            raise ValueError("No workflow was enabled.")

        # The workflow inputs.
        in_fields = ['subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')

        # Stitch together the workflows:

        # Stage the session images.
        if stg_wf:
            exec_wf.connect(
                input_spec, 'subject', stg_wf, 'input_spec.subject')
            exec_wf.connect(
                input_spec, 'session', stg_wf, 'input_spec.session')

        # Create the mask.
        if mask_wf:
            exec_wf.connect(
                input_spec, 'subject', mask_wf, 'input_spec.subject')
            exec_wf.connect(
                input_spec, 'session', mask_wf, 'input_spec.session')
            # If the input DICOM files will be staged and the mask will be created
            # rather than downloaded, then connect the staged output to the mask
            # input.
            if stg_wf and not mask_opt:
                exec_wf.connect(
                    stg_wf, 'output_spec.images', mask_wf, 'input_spec.images')

        # Register the staged images.
        if reg_wf:
            exec_wf.connect(
                input_spec, 'subject', reg_wf, 'input_spec.subject')
            exec_wf.connect(
                input_spec, 'session', reg_wf, 'input_spec.session')
            # If registration will be performed to create the realigned images,
            # then connect the mask output to the registration mask input.
            # Furthermore, if the input DICOM files will be staged, then connect
            # the staged output to the registration input.
            if not reg_opt:
                exec_wf.connect(
                    mask_wf, 'output_spec.mask', reg_wf, 'input_spec.mask')
                if stg_wf:
                    exec_wf.connect(
                        stg_wf, 'output_spec.images', reg_wf, 'input_spec.images')
                    exec_wf.connect(
                        stg_wf, 'iter_image.image', reg_wf, 'iter_image.image')

        # Model the realigned images.
        if mdl_wf:
            exec_wf.connect(
                input_spec, 'subject', mdl_wf, 'input_spec.subject')
            exec_wf.connect(
                input_spec, 'session', mdl_wf, 'input_spec.session')
            exec_wf.connect(
                mask_wf, 'output_spec.mask', mdl_wf, 'input_spec.mask')
            exec_wf.connect(
                reg_wf, 'output_spec.images', mdl_wf, 'input_spec.images')

        self._configure_nodes(exec_wf)

        self._logger.debug("Created the %s workflow." % exec_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self._depict_workflow(exec_wf)

        return exec_wf

    def _create_scan_download_workflow(self, base_dir):
        """
        Makes the XNAT session series stack NiFTI file download staging workflow.

        :param base_dir: the workflow execution directory
        :return: the new workflow
        """
        self._logger.debug("Creating the session download workflow...")

        # The Nipype workflow object.
        dl_wf = pe.Workflow(name='staging', base_dir=base_dir)

        # The workflow input.
        in_fields = ['subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self._logger.debug("The %s input node is %s with fields %s" %
                         (dl_wf.name, input_spec.name, in_fields))

        # The input scan iterator.
        iter_scan = pe.Node(IdentityInterface(fields=['scan']),
                            name='iter_scan')

        # Download each XNAT series stack file.
        dl_xfc = XNATDownload(project=project(), format='NIFTI')
        download_session = pe.Node(dl_xfc, name='download_session')
        dl_wf.connect(input_spec, 'subject', download_session, 'subject')
        dl_wf.connect(input_spec, 'session', download_session, 'session')
        dl_wf.connect(iter_scan, 'scan', download_session, 'scan')

        # The output image iterator.
        iter_image = pe.Node(IdentityInterface(fields=['image']),
                             name='iter_image')
        dl_wf.connect(download_session, 'out_file', iter_image, 'image')

        # The workflow output.
        output_spec = pe.JoinNode(IdentityInterface(fields=['images']),
                                  joinsource='iter_scan', joinfield='images', name='output_spec')
        dl_wf.connect(iter_image, 'image', output_spec, 'images')

        self._logger.debug("Created the session download workflow.")

        return dl_wf

    def _create_mask_download_workflow(self, base_dir, recon):
        """
        Makes the XNAT session mask file download workflow.

        :param base_dir: the workflow execution directory
        :param recon: the XNAT mask reconstruction label
        :return: the new workflow
        """
        self._logger.debug("Creating the session download workflow...")

        # The Nipype workflow object.
        dl_wf = pe.Workflow(name='mask', base_dir=base_dir)

        # The workflow input.
        in_fields = ['subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self._logger.debug("The %s input node is %s with fields %s" %
                         (dl_wf.name, input_spec.name, in_fields))

        # Download each XNAT series stack file.
        dl_xfc = XNATDownload(project=project(), reconstruction=recon)
        download_mask = pe.Node(dl_xfc, name='download_mask')
        dl_wf.connect(input_spec, 'subject', download_mask, 'subject')
        dl_wf.connect(input_spec, 'session', download_mask, 'session')

        # The workflow output.
        output_spec = pe.Node(IdentityInterface(fields=['mask']),
                              name='output_spec')
        dl_wf.connect(download_mask, 'out_file', output_spec, 'mask')

        self._logger.debug("Created the mask download workflow.")

        return dl_wf

    def _create_registration_download_workflow(self, base_dir, recon):
        """
        Makes the XNAT session registration images download workflow.

        :param base_dir: the workflow execution directory
        :param recon: the XNAT registration reconstruction label
        :return: the new workflow
        """
        self._logger.debug("Creating the registration download workflow...")

        # The Nipype workflow object.
        dl_wf = pe.Workflow(name='registration', base_dir=base_dir)

        # The workflow input.
        in_fields = ['subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self._logger.debug("The %s input node is %s with fields %s" %
                         (dl_wf.name, input_spec.name, in_fields))

        # Download the XNAT registration files.
        dl_xfc = XNATDownload(project=project(), reconstruction=recon)
        download_reg = pe.Node(dl_xfc, name='download_reg')
        dl_wf.connect(input_spec, 'subject', download_reg, 'subject')
        dl_wf.connect(input_spec, 'session', download_reg, 'session')

        # The workflow output.
        output_spec = pe.Node(IdentityInterface(fields=['images']),
                              name='output_spec')
        dl_wf.connect(download_reg, 'out_files', output_spec, 'images')

        self._logger.debug("Created the registration download workflow.")

        return dl_wf
