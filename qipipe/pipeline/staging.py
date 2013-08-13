import os, glob
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.io import DataSink
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces.dcmstack import DcmStack
from ..helpers.project import project
from ..interfaces import (FixDicom, Compress, Uncompress, Copy, MapCTP, XNATFind, XNATUpload)
from ..staging.staging_error import StagingError
from ..staging.staging_helper import (subject_for_directory, iter_new_visits,
    group_dicom_files_by_series)
from ..helpers import xnat_helper
from .workflow_base import WorkflowBase

import logging
logger = logging.getLogger(__name__)


def run(*inputs, **opts):
    """
    Creates a :class:`qipipe.pipeline.staging.StagingWorkflow` and runs it
    on the given inputs.
    
    :param inputs: the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` inputs
    :param opts: the :class:`qipipe.pipeline.staging.StagingWorkflow` initializer
        and :meth:`qipipe.pipeline.staging.StagingWorkflow.run` options
    :return: the :meth:`qipipe.pipeline.staging.StagingWorkflow.run` result
    """
    # Extract the run options.
    run_opts = {}
    for opt in ['dest', 'overwrite']:
        if opt in opts:
            run_opts[opt] = opts.pop(opt)
    return StagingWorkflow(**opts).run(*inputs, **run_opts)


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
    
    - Collect the id map and the compressed DICOM images into a target directory
      in collection/subject/session/series format for TCIA upload.
    
    .. _CTP: https://wiki.cancerimagingarchive.net/display/Public/Image+Submitter+Site+User%27s+Guide
    .. _DcmStack: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.dcmstack.html
    """
    
    def __init__(self, cfg_file=None, base_dir=None):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.
        
        :parameter base_dir: the workflow execution directory
            (default a new temp directory)
        :parameter cfg_file: the optional workflow inputs configuration file
        """
        super(StagingWorkflow, self).__init__(logger, cfg_file)
        
        self.dicom_sequence = self._create_dicom_sequence(base_dir=base_dir)
        """
        The DICOM file processing workflow sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        """
        
        self.stack_sequence = self._create_stack_sequence(base_dir=base_dir)
        """
        The scan 3-D NiFTI stack workflow sequence described in
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
        
        :Note: If the `overwrite` option is set, then existing XNAT subjects
            which correspond to subjects in the input directories are deleted.
        
        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories to stage
        :param opts: the following workflow execution options:
        :keyword dest: the TCIA upload destination directory (default current
            working directory)
        :keyword overwrite: flag indicating whether to replace existing XNAT
            subjects (default False)
        :return: the {subject: {session: stack files}} dictionary
        """
        # Group the new DICOM files into a
        # {subject: {session: [(series, dicom_files), ...]}} dictionary..
        overwrite = opts.get('overwrite', False)
        stg_dict = self._detect_new_visits(collection, *inputs,
            overwrite=overwrite)
        if not stg_dict:
            return []
        
        # The staging location.
        if opts.has_key('dest'):
            dest = os.path.abspath(opts['dest'])
        else:
            dest = os.path.join(os.getcwd(), 'data')
        
        # The workflow subjects.
        subjects = stg_dict.keys()
        # Make the TCIA subject map.
        self._create_subject_map(collection, subjects, dest)
        
        # The {subject: {session: stacks}} dictionary result.
        stacks_dict = defaultdict(lambda: defaultdict(list))
        
        series_cnt = sum(map(len, stg_dict.itervalues()))
        logger.debug("Staging %d new %s series from %d subjects in %s..." %
            (series_cnt, collection, len(subjects), dest))
        # The TCIA upload prep DICOM staging area.
        dicom_dest = os.path.join(dest, 'dicom')
        # The series stacks staging area.
        stack_dest = os.path.join(dest, 'stacks')
        # The subject workflow.
        for sbj, sess_dict in stg_dict.iteritems():
            # The session workflow.
            logger.debug("Staging subject %s..." % sbj)
            for sess, ser_dict in sess_dict.iteritems():
                logger.debug("Staging %s session %s..." % (sbj, sess))
                # The series workflow.
                for ser, dicom_files in ser_dict.iteritems():
                    staged = self._stage_dicom(collection, sbj, sess, ser,
                        dicom_files, dicom_dest)
                    stack = self._create_stack(sbj, sess, ser, staged,
                        stack_dest)
                    stacks_dict[sbj][sess].append(stack)
                logger.debug("Staged %s session %s." % (sbj, sess))
            logger.debug("Staged subject %s." % sbj)
        logger.debug("Staged %d new %s series from %d subjects in %s." %
            (series_cnt, collection, len(subjects), dest))
        
        # Return the {{subject: {session: stacks}} dictionary.
        return stacks_dict
    
    def _detect_new_visits(self, collection, *inputs, **opts):
        """
        Detects which AIRC visits in the given input directories have not yet
        been stored into XNAT. The new visit images are grouped by series.
        
        :param collection: the AIRC image collection name
        :param inputs: the AIRC source subject directories
        :param opts: the following options:
        :keyword overwrite: flag indicating whether to replace existing XNAT
            subjects (default False)
        :return: the {subject: {session: {series: [dicom files]}}} dictionary
        """
        # If the overwrite option is set, then delete existing subjects.
        if opts.get('overwrite'):
            subjects = [subject_for_directory(collection, d) for d in inputs]
            xnat_helper.delete_subjects(*subjects)
        
        # Collect the new AIRC visits into (subject, session, dicom_files)
        # tuples.
        new_visits = list(iter_new_visits(collection, *inputs))
        
        # If there are no new images, then bail.
        if not new_visits:
            logger.info("No new images were detected.")
            return []
        logger.debug("%d new visits were detected" % len(new_visits))
        
        # Group the DICOM files by series.
        return self._group_sessions_by_series(*new_visits)
    
    def _create_subject_map(self, collection, subjects, dest):
        """
        Maps each QIN Patient ID to a TCIA Patient ID for upload using CTP.
        """
        logger.debug("Creating the TCIA subject map in %s..." % dest)
        map_ctp = MapCTP(collection=collection, patient_ids=subjects, dest=dest)
        result = map_ctp.run()
        logger.debug("Created the TCIA subject map %s." % result.outputs.out_file)
    
    def _create_dicom_sequence(self, base_dir=None):
        """
        Makes the DICOM processing sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        
        :param base_dir: the workflow execution directory
            (default is a new temp directory)
        :return: the new workflow
        """
        logger.debug("Creating the DICOM processing workflow...")
        
        # The Nipype workflow object.
        workflow = pe.Workflow(name='dicom_staging', base_dir=base_dir)
        
        # The sequence input.
        in_fields = ['collection', 'subject', 'session', 'series', 'dicom_file', 'dest']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
            name='input_spec')
        logger.debug("The %s workflow input node is %s with fields %s" %
            (workflow.name, input_spec.name, in_fields))
        
        # Fix the AIRC DICOM tags.
        fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
        workflow.connect(input_spec, 'collection', fix_dicom, 'collection')
        workflow.connect(input_spec, 'subject', fix_dicom, 'subject')
        workflow.connect(input_spec, 'dicom_file', fix_dicom, 'in_file')
        
        # Compress the corrected DICOM file.
        compress_dicom = pe.Node(Compress(), name='compress_dicom')
        workflow.connect(fix_dicom, 'out_file', compress_dicom, 'in_file')
        workflow.connect(input_spec, 'dest', compress_dicom, 'dest')
        
        # # Upload the compressed DICOM file to XNAT.
        # upload_dicom = pe.Node(XNATUpload(project=project(), format='DICOM'),
        #     name='upload_dicom')
        # workflow.connect(input_spec, 'subject', upload_dicom, 'subject')
        # workflow.connect(input_spec, 'session', upload_dicom, 'session')
        # workflow.connect(input_spec, 'series', upload_dicom, 'scan')
        # workflow.connect(compress_dicom, 'out_file', upload_dicom, 'in_files')
        
        logger.debug("Created the %s workflow sequence." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(workflow)
        
        return workflow
    
    def _create_stack_sequence(self, base_dir=None):
        """
        Makes the series stack processing sequence described in
        :class:`qipipe.pipeline.staging.StagingWorkflow`.
        
        :param base_dir: the workflow execution directory
            (default is a new temp directory)
        :return: the new workflow
        """
        logger.debug("Creating the staging workflow...")
        
        # The Nipype workflow.
        workflow = pe.Workflow(name='stack_staging', base_dir=base_dir)
        
        # The sequence input.
        in_fields = ['collection', 'subject', 'session', 'series', 'dicom_files', 'dest']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
            name='input_spec')
        logger.debug("The %s workflow input node is %s with fields %s" %
            (workflow.name, input_spec.name, in_fields))
        
        # Uncompress the corrected DICOM file.
        uncompress_dicom = pe.MapNode(Uncompress(), iterfield='in_file',
            name='uncompress_dicom')
        workflow.connect(input_spec, 'dicom_files', uncompress_dicom, 'in_file')
        workflow.connect(input_spec, 'dest', uncompress_dicom, 'dest')
        
        # Stack the scan.
        stack = pe.Node(DcmStack(embed_meta=True,
            out_format="series%(SeriesNumber)03d"), name='stack')
        workflow.connect(uncompress_dicom, 'out_file', stack, 'dicom_files')
        
        # Store the stack in XNAT.
        upload_stack = pe.Node(XNATUpload(project=project(), format='NIFTI'),
            name='upload_stack')
        workflow.connect(input_spec, 'subject', upload_stack, 'subject')
        workflow.connect(input_spec, 'session', upload_stack, 'session')
        workflow.connect(input_spec, 'series', upload_stack, 'scan')
        workflow.connect(stack, 'out_file', upload_stack, 'in_files')
        
        # Copy the stack file to the staging area.
        copy_stack = pe.Node(Copy(), name='copy_stack')
        workflow.connect(input_spec, 'dest', copy_stack, 'dest')
        workflow.connect(stack, 'out_file', copy_stack, 'in_file')
        
        self._configure_nodes(workflow)
        
        logger.debug("Created the %s workflow sequence." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if logger.level <= logging.DEBUG:
            self._depict_workflow(workflow)
        
        return workflow
    
    def _stage_dicom(self, collection, subject, session, series, dicom_files, dest):
        """
        Stages the given series DICOM files, uploads them to XNAT and places the
        staged DICOM files in the given destination as described in
        :meth:`qipipe.pipeline.staging.StagingWorkflow.run`.
        
        :return: the staged DICOM files
        """
        # Make the staging directory.
        ser_dest = self._make_series_staging_directory(dest, subject, session,
            series)
        logger.debug("Staging %d %s %s series %s DICOM files in %s..." %
            (len(dicom_files), subject, session, series, ser_dest))
        
        # Make the XNAT scan DICOM resource.
        cr_rsc = XNATFind(project=project(), subject=subject,
            session=session, scan=series, resource='DICOM', create=True)
        cr_rsc.run()
        
        # Set the inputs.
        dicom_input = self.dicom_sequence.get_node('input_spec')
        dicom_input.inputs.collection = collection
        dicom_input.inputs.subject = subject
        dicom_input.inputs.session = session
        dicom_input.inputs.series = series
        dicom_input.inputs.dest = ser_dest
        dicom_input.iterables = ('dicom_file', dicom_files)
        
        # Execute the workflow.
        self._run_workflow(self.dicom_sequence)
        out_files = glob.glob(ser_dest + '/*.dcm.gz')
        
        # Upload the DICOM files.
        with xnat_helper.connection() as xnat:
            xnat.upload(project(), subject, session, series, *out_files)
        
        logger.debug("Staged %d %s %s series %s DICOM files in %s." %
            (len(out_files), subject, session, series, ser_dest))
        
        return out_files
    
    def _create_stack(self, subject, session, series, dicom_files, dest):
        """Stacks the given series DICOM files into a 3-D NiFTI image,
        uploads the stack image file to XNAT and places the file in
        in the given destination as described in
        :meth:`qipipe.pipeline.staging.StagingWorkflow.run`."""
        # Make the staging directory.
        ser_dest = self._make_series_staging_directory(dest, subject, session,
            series)
        logger.debug("Staging the %s %s series %s stacks in %s..." %
            (subject, session, series, ser_dest))
        
        # Make the XNAT scan NIFTI resource.
        cr_rsc = XNATFind(project=project(), subject=subject,
            session=session, scan=series, resource='NIFTI', create=True)
        cr_rsc.run()
        
        # Set the inputs.
        stack_input = self.stack_sequence.get_node('input_spec')
        stack_input.inputs.subject = subject
        stack_input.inputs.session = session
        stack_input.inputs.series = series
        stack_input.inputs.dicom_files = dicom_files
        stack_input.inputs.dest = ser_dest
        
        # Execute the workflow.
        self._run_workflow(self.stack_sequence)
        logger.debug("Staged the %s %s series %s stack in %s." %
            (subject, session, series, ser_dest))
        
        # Return the stack file in the staging area.
        return self._stack_filename(dest, series)
    
    def _stack_filename(self, dest, series):
        """
        :param dest: the parent directory
        :param series: the series number
        :return: the stack filename
        """
        fname = "series%03d.nii.gz" % series
        
        return os.path.join(dest, fname)
    
    def _group_sessions_by_series(self, *session_specs):
        """
        Creates the staging dictionary for the new images in the given sessions.
        
        :param session_specs: the (subject, session, dicom_files) tuples to group
        :return: the {subject: {session: {series: [dicom files]}}} dictionary
        """
        
        # The {subject: {session: {series: [dicom files]}}} output.
        stg_dict = defaultdict(dict)
        
        for sbj, sess, dcm_file_iter in session_specs:
            # Group the session DICOM input files by series.
            ser_dcm_dict = group_dicom_files_by_series(dcm_file_iter)
            if not ser_dcm_dict:
                raise StagingError("No DICOM files were detected in the "
                    "%s %s session source directory." % (sbj, sess))
            # Collect the (series, dicom_files) tuples.
            stg_dict[sbj][sess] = ser_dcm_dict
        
        return stg_dict
    
    def _make_series_staging_directory(self, dest, subject, session, series):
        """
        Returns the dest/subject/session/series directory path in which to place
        DICOM files for TCIA upload. Creates the directory, if necessary.
        
        :return: the target series directory path
        """
        path = os.path.join(dest, subject, session, str(series))
        if not os.path.exists(path):
            os.makedirs(path)
        
        return path
