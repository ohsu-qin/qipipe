"""The qipipeline L{run} function is the OHSU QIN pipeline facade."""

import os, glob
import tempfile
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.dcmstack import DcmStack
from ..interfaces import FixDicom, Compress, GroupDicom, MapCTP, XNATUpload
from ..staging.staging_helper import SUBJECT_FMT,iter_new_visits, group_dicom_files_by_series
from numpy import array

import logging
logger = logging.getLogger(__name__)

def run(collection, *subject_dirs, **opts):
    """
    Runs the OHSU QIN pipeline on the the given AIRC subject directories as follows:
        - Detects which AIRC visits have not yet been stored into XNAT
        - Groups the input DICOM images into series.
        - Fixes each input DICOM header for import into CTP.
        - Uploads the fixed DICOM file into XNAT.
        - Makes the CTP subject id map.
        - Stacks each new series as a NiFTI file using DcmStack.
        - Uploads each new series stack into XNAT.
        - Registers each new series.
        - Uploads each new registered series into XNAT.
    
    The supported AIRC collections are defined L{qipipe.staging.airc_collection}.

    The options include the C{dest} destination directory and the workflow creation
    C{base_dir} working area option. The default destination is the current working
    directory. The destination directory is populated with the CTP import staging files.
    
    @param collection: the AIRC image collection name
    @param dest: the destination directory
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: additional workflow options
    """

    return QIPipeline(collection).run(*subject_dirs, **opts)
    
class QIPipeline(object):
    def __init__(self, collection):
        """
        @param collection: the AIRC image collection
        """
        self.collection = collection
    
    def run(self, *subject_dirs, **opts):
        """
        Runs the qipipeline workflow on the the given AIRC subject directories.

        The options are described in L{qipipe.qipipeline.run}.
        
        @param dest: the CTP staging location
        @param subject_dirs: the AIRC source subject directories to stage
        @param opts: additional workflow options
        @return: the new XNAT session labels
        """

        # Collect the new AIRC visits.
        new_visits = list(iter_new_visits(self.collection, *subject_dirs))
        if not new_visits:
            return []
        
        # The XNAT subjects with new visits.
        subjects = []
        # The new XNAT sessions.
        sessions = []
        # The (subject, session, series, dicom files) inputs.
        ser_specs = []
        for sbj, sess, dcm_file_iter in new_visits:
            subjects.append(sbj)
            sessions.append(sess)
            # Group the session DICOM input files by series.
            ser_dcm_dict = group_dicom_files_by_series(dcm_file_iter)
            # Collect the (subject, session, series, dicom_files) tuples.
            for ser, dcm in ser_dcm_dict.iteritems():
                logger.debug("The QIN workflow will iterate over subject %s session %s series %s." % (sbj, sess, ser))
                ser_specs.append((sbj, sess, ser, dcm))

        # Make a default workflow base directory, if necessary.
        if 'work' in opts:
            opts['base_dir'] = opts['work']
        else:
            opts['base_dir'] = tempfile.mkdtemp()
        
        # Make the workflow.
        wf = self._create_workflow(ser_specs, **opts)
        
        # The staging location.
        dest = os.path.abspath(opts.pop('dest', os.getcwd()))
        
        # Set the top-level workflow inputs.
        wf.inputs.input_spec.collection = self.collection
        wf.inputs.input_spec.dest = dest
        wf.inputs.input_spec.subjects = subjects
        
        # Diagram the workflow graph.
        grf = os.path.join(wf.base_dir, 'qipipeline.dot')
        wf.write_graph(dotfilename=grf)
        logger.debug("The workflow graph is depicted at %s.png." % grf)
        
        # Run the workflow.
        wf.run()
        
        # Return all new sessions.
        return sessions
        
    def _create_workflow(self, series_spec_inputs, **opts):
        """
        @param series_spec_inputs: the subjects, sessions, series, and series dicom_files dictionary
        @param opts: additional workflow options
        @return: the OHSU QIN workflow
        """
        
        # The OHSU QIN workflow.
        msg = 'Creating the workflow'
        if opts:
            msg = msg + ' with options %s...' % opts
        logger.debug("%s...", msg)
        wf = pe.Workflow(name='qipipeline', **opts)
        
        # The workflow input.
        input_spec = pe.Node(IdentityInterface(fields=['collection', 'dest', 'subjects']),
            name='input_spec')
        
        # The series spec node. The node input is a (subject, session, series, dicom_files)
        # tuple. The node output is the disaggregated subject, session, series and dicom_files.
        # This node iterates over each series spec tuple.
        ser_spec_func = Function(input_names=['thing'],
            output_names=['subject', 'session', 'series', 'dicom_files'], function=_identity)
        series_spec = pe.Node(ser_spec_func, name='series_spec')
        series_spec.iterables = ('thing', series_spec_inputs)
        
        # Map each QIN Patient ID to a CTP Patient ID.
        map_ctp = pe.Node(MapCTP(), name='map_ctp')
        
        # The CTP staging directory factory.
        ctp_dir_func = Function(input_names=['dest', 'subject', 'session', 'series'],
            output_names=['out_dir'], function=_ctp_series_directory)
        ctp_dir = pe.Node(ctp_dir_func, name='ctp_dir')
        
        # Fix the AIRC DICOM tags.
        fix_dicom = pe.Node(FixDicom(), name='fix_dicom')
        fix_dicom.inputs.collection = self.collection
        
        # Compress the fixed files.
        compress = pe.MapNode(Compress(), iterfield='in_file', name='compress')
        
        # Store the DICOM files in XNAT.
        store_dicom = pe.Node(XNATUpload(project='QIN', format='DICOM'), name='store_dicom')
        
        # Stack the series.
        stack = pe.Node(DcmStack(embed_meta=True, out_format="series%(SeriesNumber)03d"),
            name='stack')
        
        # Store the series stack in XNAT.
        store_stack=pe.Node(XNATUpload(project='QIN'), name='store_stack')
        
        # Build the workflow.
        wf.connect([
            (input_spec, map_ctp, [('collection', 'collection'), ('subjects', 'patient_ids'), ('dest', 'dest')]),
            (input_spec, fix_dicom, [('collection', 'collection')]),
            (input_spec, ctp_dir, [('dest', 'dest')]),
            (series_spec, ctp_dir, [('subject', 'subject'), ('session', 'session'), ('series', 'series')]),
            (series_spec, fix_dicom, [('subject', 'subject'), ('dicom_files', 'in_files')]),
            (series_spec, store_dicom, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
            (series_spec, store_stack, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
            (ctp_dir, compress, [('out_dir', 'dest')]),
            (fix_dicom, compress, [('out_files', 'in_file')]),
            (compress, store_dicom, [('out_file', 'in_files')]),
            (fix_dicom, stack, [('out_files', 'dicom_files')]),
            (stack, store_stack, [('out_file', 'in_files')])])
        
        return wf

def _identity(thing):
    """
    This silly workflow glue function returns the input parameter. This function is used to extract output
    parameters from an iterable input parameter.
    """
    return thing

def _ctp_series_directory(dest, subject, session, series):
    """
    @return: the dest/subject/session/series directory path
    """
    import os
    return os.path.join(dest, subject, session, str(series))
