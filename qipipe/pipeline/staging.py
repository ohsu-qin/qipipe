import os
import logging
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.dcmstack import DcmStack
from .. import project
from ..interfaces import (Gate, FixDicom, Compress, MapCTP, XNATUpload)
from ..staging.staging_error import StagingError
from ..staging.staging_helper import (subject_for_directory, iter_new_visits,
                                      group_dicom_files_by_series)
from ..helpers import xnat_helper
from .workflow_base import WorkflowBase
from ..helpers.logging_helper import logger


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.staging.StagingWorkflow` and runs it
    on the given inputs.

    :param inputs: the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` inputs
    :param opts: the :class:`qipipe.pipeline.staging.StagingWorkflow` initializer
        and :meth:`qipipe.pipeline.staging.StagingWorkflow.run` options
    :return: the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` result
    """
    cfg_file = opts.pop('cfg_file', None)
    
    stg_wf = StagingWorkflow(**opts)
    
    return stg_wf.run(*inputs, **opts)


class StagingWorkflow(WorkflowBase):

    """
    The StagingWorkflow class builds and executes the staging Nipype workflow.
    The staging workflow includes the following steps:

    - Group the input DICOM images into series.

    - Fix each input DICOM file header using the
      :class:`qipipe.interfaces.fix_dicom.FixDicom` interface.

    - Compress each corrected DICOM file.

    - Upload each compressed DICOM file into XNAT.

    - Stack each new series's 2-D DICOM files into a 3-D series NiFTI file
      using the DcmStack_ interface.

    - Upload each new series stack into XNAT.

    - Make the CTP_ QIN-to-TCIA subject id map.

    - Collect the id map and the compressed DICOM images into a target
      directory in collection/subject/session/series format for TCIA
      upload.

    The staging workflow input is the ``input_spec`` node consisting of
    the following input fields:

    - ``subject``: the subject name

    - ``session``: the session name

    - ``series``: the scan number
    
    The staging workflow has two iterables:
    
    - the ``input_spec`` ``series`` and ``dest`` fields
    
    - the ``iter_dicom`` ``dicom_file`` field
    
    These iterables must be set prior to workflow execution. The
    ``input_spec`` iterables is set to the session scan numbers.
    
    The ``iter_dicom`` node *itersource* is the ``iter_series.series``
    field. The ``iter_dicom.dicom_file`` iterables is set to the
    {series: [DICOM files]} dictionary.
   
    The staging workflow output is the ``output_spec`` node consisting
    of the following output field:

    - ``image``: the session series stack NiFTI image file

    .. _CTP: https://wiki.cancerimagingarchive.net/display/Public/Image+Submitter+Site+User%27s+Guide
    .. _DcmStack: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.dcmstack.html
    """

    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :parameter opts: the following keword options:
        :keyword project: the XNAT project (default ``QIN``)
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        """
        cfg_file = opts.get('cfg_file')
        super(StagingWorkflow, self).__init__(logger(__name__), cfg_file)

        # Set the XNAT project.
        if 'project' in opts:
            project(opts['project'])
            self._logger.debug("Set the XNAT project to %s." % project())

        # Make the workflow.
        base_dir = opts.get('base_dir')
        self.workflow = self._create_workflow(base_dir=base_dir)
        """
        The staging workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """

    def run(self, collection, *inputs, **opts):
        """
        Runs the staging workflow on the new AIRC visits in the given
        input directories.

        The new DICOM files to upload to TCIA are placed in the destination
        `dicom` subdirectory in the following hierarchy:

            /path/to/dest/dicom/
                subject/
                    session/
                        series/
                            file.dcm.gz ...

        where *file* is the DICOM file name.

        The new series stack NiFTI files are placed in the destination
        `stacks` subdirectory in the following hierarchy:

            /path/to/dest/stacks/
                subject/
                    session/
                        file.nii.gz ...

        If the *workflow* parameter is set, then that execution workflow is
        required to include a node named ``input_spec`` with inputs ``subject``
        and ``session`` which connect to the respective inputs in the child
        staging workflow.

        The return value is a *{subject: {session: [scans]}}* XNAT name
        dictionary for each processed session.

        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories to stage
        :param opts: the following workflow execution options:
        :keyword dest: the TCIA staging destination directory (default is a
            subdirectory named ``staged`` in the current working directory)
        :keyword workflow: the workflow to run (default is the standard
            staging workflow ``workflow`` instance variable)
        :return: the XNAT *{subject: {session: [scans]}}* dictionary
        """
        # Validate that there is a collection
        if not collection:
            raise ValueError('Staging is missing the AIRC collection name')
        
        # Group the new DICOM files into a
        # {subject: {session: [(series, dicom_files), ...]}} dictionary.
        stg_dict = self._detect_visits(collection, *inputs)
        if not stg_dict:
            return {}

        # The staging location.
        dest_opt = opts.pop('dest', None)
        if dest_opt:
            dest = os.path.abspath(dest_opt)
        else:
            dest = os.path.join(os.getcwd(), 'staged')

        # The workflow subjects.
        subjects = stg_dict.keys()
        # Make the TCIA subject map.
        self._create_subject_map(collection, subjects, dest)

        series_cnt = sum(map(len, stg_dict.itervalues()))
        self._logger.debug(
            "Staging %d new %s series from %d subjects in %s..." %
            (series_cnt, collection, len(subjects), dest))

        # The execution workflow (see method doc)
        exec_wf = opts.get('workflow', self.workflow)
        # Connect the series and DICOM iterators. This must be done before
        # using the workflow.
        iter_series = exec_wf.get_node('iter_series')
        iter_dicom = exec_wf.get_node('iter_dicom')
        exec_wf.connect(iter_series, 'series', iter_dicom, 'series')
        iter_series.synchronize = True
        iter_dicom.itersource = ('iter_series', 'series')
        
        # Run the workflow for each session.
        for sbj, sess_dict in stg_dict.iteritems():
            self._logger.debug("Staging subject %s..." % sbj)
            for sess, ser_dict in sess_dict.iteritems():
                self._logger.debug("Staging %s session %s..." % (sbj, sess))
                self._stage_session(collection, sbj, sess,
                                    ser_dict, dest, exec_wf)
                self._logger.debug("Staged %s session %s." % (sbj, sess))
            self._logger.debug("Staged subject %s." % sbj)
        self._logger.debug("Staged %d new %s series from %d subjects in %s." %
                         (series_cnt, collection, len(subjects), dest))

        # Return the {subject: {session: [scans]}} dictionary.
        output_dict = defaultdict(dict)
        for sbj, sess_dict in stg_dict.iteritems():
            for sess, ser_dict in sess_dict.iteritems():
                output_dict[sbj][sess] = ser_dict.keys()
        return output_dict

    def _detect_visits(self, collection, *inputs):
        """
        Detects the new AIRC visits in the given input directories. The visit
        images are grouped by series.

        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories
        :return: the *{subject: {session: {series: [dicom files]}}}* dictionary
        """
        # Collect the AIRC visits into (subject, session, dicom_files)
        # tuples.
        visits = list(iter_new_visits(collection, *inputs))

        # If no images were detected, then bail.
        if not visits:
            self._logger.info("No visits were detected in the input directories.")
            return {}
        self._logger.debug("%d visits were detected" % len(visits))

        # Group the DICOM files by series.
        return self._group_sessions_by_series(*visits)

    def _create_subject_map(self, collection, subjects, dest):
        """
        Maps each QIN Patient ID to a TCIA Patient ID for upload using CTP.
        """
        self._logger.debug("Creating the TCIA subject map in %s..." % dest)
        map_ctp = MapCTP(
            collection=collection, patient_ids=subjects, dest=dest)
        result = map_ctp.run()
        self._logger.debug("Created the TCIA subject map %s." %
                          result.outputs.out_file)

    def _stage_session(self, collection, subject, session, ser_dicom_dict,
                       dest, exec_wf):
        """
        Stages the given session's series DICOM files as described in
        :meth:`qipipe.pipeline.staging.StagingWorkflow.run`.
        """
        # Collect the (series, destination) tuples.
        ser_dest_tuples = []
        for series, dicom_files in ser_dicom_dict.iteritems():
            # Make the staging directories. Do this before running the
            # workflow in order to avoid a directory creation race
            # condition for distributed nodes that write to the series
            # staging directory.
            ser_dest = self._make_series_staging_directory(dest, subject,
                                                           session, series)
            ser_dest_tuples.append((series, ser_dest))
            self._logger.debug(
                "Staging %d %s %s series %s DICOM files in %s..." %
                (len(dicom_files), subject, session, series, ser_dest))

        # Transpose the tuples into iterable lists.
        sers, dests = map(list, zip(*ser_dest_tuples))
        ser_iterables = dict(series=sers, dest=dests).items()

        # Set the inputs.
        input_spec = exec_wf.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.collection = collection
        
        iter_series = exec_wf.get_node('iter_series')
        iter_series.iterables = ser_iterables

        iter_dicom = exec_wf.get_node('iter_dicom')
        iter_dicom.iterables = ('dicom_file', ser_dicom_dict)

        # Execute the workflow.
        self._run_workflow(exec_wf)
        self._logger.debug("Staged %d %s %s series %d DICOM files in %s." %
                         (len(dicom_files), subject, session, series, ser_dest))

    def _create_workflow(self, base_dir=None):
        """
        Makes the staging workflow described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.

        :param base_dir: the workflow execution directory
            (default is a new temp directory)
        :return: the new workflow
        """
        self._logger.debug("Creating the DICOM processing workflow...")

        # The Nipype workflow object.
        workflow = pe.Workflow(name='staging', base_dir=base_dir)

        # The workflow input.
        in_fields = ['collection', 'subject', 'session']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')
        self._logger.debug("The %s workflow input node is %s with fields %s" %
                         (workflow.name, input_spec.name, in_fields))
        
        iter_series_fields = ['series', 'dest']
        iter_series = pe.Node(IdentityInterface(fields=iter_series_fields),
                                  name='iter_series')
        self._logger.debug("The %s workflow series iterable node is %s with fields %s" %
                         (workflow.name, iter_series.name, iter_series_fields))
  
        # The DICOM file iterator.
        iter_dicom_fields = ['series', 'dicom_file']
        iter_dicom = pe.Node(IdentityInterface(fields=iter_dicom_fields),
                             name='iter_dicom')
        self._logger.debug("The %s workflow DICOM iterable node is %s with iterable"
                           " source %s and iterables ('%s', {%s: [%s]})" %
                           (workflow.name, iter_dicom.name, iter_series.name,
                           'dicom_file', 'series', 'DICOM files'))

        # Fix the AIRC DICOM tags.
        fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
        workflow.connect(input_spec, 'collection', fix_dicom, 'collection')
        workflow.connect(input_spec, 'subject', fix_dicom, 'subject')
        workflow.connect(iter_dicom, 'dicom_file', fix_dicom, 'in_file')

        # Compress the corrected DICOM file.
        compress_dicom = pe.Node(Compress(), name='compress_dicom')
        workflow.connect(fix_dicom, 'out_file', compress_dicom, 'in_file')
        workflow.connect(iter_series, 'dest', compress_dicom, 'dest')

        # Upload the compressed DICOM files to XNAT.
        # Since only one upload task can run at a time, this upload node is
        # a JoinNode that collects the iterated series DICOM files.
        upload_dicom = pe.JoinNode(
            XNATUpload(project=project(), format='DICOM'),
            joinsource='iter_dicom', joinfield='in_files', name='upload_dicom')
        workflow.connect(input_spec, 'subject', upload_dicom, 'subject')
        workflow.connect(input_spec, 'session', upload_dicom, 'session')
        workflow.connect(iter_series, 'series', upload_dicom, 'scan')
        workflow.connect(compress_dicom, 'out_file', upload_dicom, 'in_files')

        # Stack the scan.
        stack_xfc = DcmStack(
            embed_meta=True, out_format="series%(SeriesNumber)03d")
        stack = pe.JoinNode(
            stack_xfc, joinsource='iter_dicom', joinfield='dicom_files',
            name='stack')
        workflow.connect(fix_dicom, 'out_file', stack, 'dicom_files')

        # # TODO - remove if unnecessary
        # 
        # # Gate the stack upload by the DICOM upload to ensure that only one
        # # upload happens at a time.
        # gate_upload_stack_xfc = Gate(fields=['image', 'xnat_files'])
        # gate_upload_stack = pe.Node(
        #     gate_upload_stack_xfc, name='gate_upload_dicom')
        # workflow.connect(
        #     upload_dicom, 'xnat_files', gate_upload_stack, 'xnat_files')
        # workflow.connect(stack, 'out_file', gate_upload_stack, 'image')

        # Upload the stack to XNAT.
        upload_stack = pe.Node(XNATUpload(project=project(), format='NIFTI'),
                               name='upload_stack')
        workflow.connect(input_spec, 'subject', upload_stack, 'subject')
        workflow.connect(input_spec, 'session', upload_stack, 'session')
        workflow.connect(iter_series, 'series', upload_stack, 'scan')
        workflow.connect(stack, 'out_file', upload_stack, 'in_files')

        # The output is the stack file.
        output_spec = pe.Node(Gate(fields=['image']),
                              name='output_spec')
        workflow.connect(stack, 'out_file', output_spec, 'image')

        self._configure_nodes(workflow)

        self._logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self._depict_workflow(workflow)

        return workflow

    def _group_sessions_by_series(self, *session_specs):
        """
        Creates the staging dictionary for the new images in the given
        sessions.

        :param session_specs: the *(subject, session, dicom_files)* tuples
            to group
        :return: the *{subject: {session: {series: [dicom files]}}}*
            dictionary
        """

        # The {subject: {session: {series: [dicom files]}}} output.
        stg_dict = defaultdict(dict)

        for sbj, sess, dcm_file_iter in session_specs:
            # Group the session DICOM input files by series.
            ser_dcm_dict = group_dicom_files_by_series(dcm_file_iter)
            if not ser_dcm_dict:
                raise StagingError("No DICOM files were detected in the "
                                   "%s %s session source directory." %
                                   (sbj, sess))
            # Collect the (series, dicom_files) tuples.
            stg_dict[sbj][sess] = ser_dcm_dict

        return stg_dict

    def _make_series_staging_directory(self, dest, subject, session, series):
        """
        Returns the dest/subject/session/series directory path in which to
        place DICOM files for TCIA upload. Creates the directory, if
        necessary.

        :return: the target series directory path
        """
        path = os.path.join(dest, subject, session, str(series))
        if not os.path.exists(path):
            os.makedirs(path)

        return path


def _join_files(*fnames):
    import os

    return os.path.join(*fnames)
