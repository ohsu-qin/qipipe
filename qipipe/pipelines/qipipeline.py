"""The qipipeline L{run} function is the OHSU QIN pipeline facade."""

import os, glob
import tempfile
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.dcmstack import DcmStack
from ..interfaces import FixDicom, Compress, GroupDicom, MapCTP, Glue, XNATUpload
from ..staging.staging_helper import SUBJECT_FMT,iter_new_visits, group_dicom_files_by_series
from numpy import array

import logging
logger = logging.getLogger(__name__)

class PipelineError(Exception):
    pass

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

    The options include the following:
        - C{components}: the L{QIPipeline.COMPONENTS} to run (default all)
        - the L{QIPipeline.run} options
    
    The destination directory is populated with the CTP import staging files.
    
    @param collection: the AIRC image collection name
    @param dest: the destination directory
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: additional workflow options
    """

    components = opts.pop('components', [])
    qip = QIPipeline(collection, *components)
    return qip.run(*subject_dirs, **opts)
    
class QIPipeline(object):
    """The OHSU QIN pipeline."""
    
    STAGING = 'staging'
    """The TCIA staging workflow component."""
    
    STACK = 'stack'
    """The DICOM series stack component."""
    
    REGISTRATION = 'registration'
    """The registration component."""
    
    DCE = 'dce'
    """The DCE workflow component."""
    
    COMPONENTS = set([STAGING, STACK, REGISTRATION, DCE])
    """The workflow components."""
    
    def __init__(self, collection, *components):
        """
        @param collection: the AIRC image collection
        @param components: the pipeline connection L{QIPipeline.COMPONENTS} (default all)
        """
        self.collection = collection
        if components:
            bad = set(components) - QIPipeline.COMPONENTS
            if bad:
                raise PipelineError("Workflow component not recognized: %s" % bad)
            self.components = components
        else:
            self.components = QIPipeline.COMPONENTS
    
    def run(self, *subject_dirs, **opts):
        """
        Runs the qipipeline workflow on the the given AIRC subject directories.

        The options include the following:
            - C{dest}: the destination directory (default current working directory)
            - C{work}: the pipeline execution work area
        
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

        # The work option is the workflow base directory.
        if 'work' in opts:
            opts['base_dir'] = opts.pop('work')
        
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
        
    def _create_workflow(self, series_specs, **opts):
        """
        @param series_spec_inputs: the (subject, session, series, dicom files) inputs
        @param opts: additional workflow options
        @return: the OHSU QIN workflow
        """
        
        # The OHSU QIN workflow.
        msg = 'Creating the workflow'
        if opts:
            msg = msg + ' with options %s...' % opts
        logger.debug("%s...", msg)
        wf = pe.Workflow(name='qipipeline', **opts)
        
        # The series specification.
        series_spec = self._create_series_spec_node(series_specs)
        
        # The staging connections.
        wf.connect(self._create_staging(series_spec))

        # Pipe the fix_dicom staging node to the stack input.
        if QIPipeline.STACK in self.components:
            fix_dicom = wf.get_node('fix_dicom')
            fix2stack = pe.Node(IdentityInterface(fields=['dicom_files']), name='fix2stack')
            wf.connect([(fix_dicom, fix2stack, [('out_files', 'dicom_files')])])
            # The stack connections.
            wf.connect(self._create_stack(series_spec, fix2stack))
        
        # Register the stack files.
        if QIPipeline.REGISTRATION in self.components:
            pass
        
        # Run the DCE pipeline on the DICOM input.
        if QIPipeline.DCE in self.components:
            pass
        
        return wf
    
    def _create_series_spec_node(self, series_specs):
        """
        Builds the series spec node. The node input is a (subject, session, series, dicom_files)
        tuple. The node output is the disaggregated subject, session, series and dicom_files.
        This node iterates over each input series spec tuple.
        
        @param series_specs: the (subject, session, series, dicom files) inputs
        @return: the series spec node
        """
        glue = Glue(input_names=['series_spec'], output_names=['subject', 'session', 'series', 'dicom_files'])
        node = pe.Node(glue, name='series_spec')
        node.iterables = ('series_spec', series_specs)
        return node
        
    def _create_staging(self, series_spec):
        """
        Creates the staging connections.
        
        @param series_spec: the series hierarchy node
        @return: the staging workflow connections
        """
        
        # The workflow input.
        input_spec = pe.Node(IdentityInterface(fields=['collection', 'dest', 'subjects']),
            name='input_spec')
                
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
        
        # Return the connections.
        return [
            (input_spec, map_ctp, [('collection', 'collection'), ('subjects', 'patient_ids'), ('dest', 'dest')]),
            (input_spec, fix_dicom, [('collection', 'collection')]),
            (input_spec, ctp_dir, [('dest', 'dest')]),
            (series_spec, ctp_dir, [('subject', 'subject'), ('session', 'session'), ('series', 'series')]),
            (series_spec, fix_dicom, [('subject', 'subject'), ('dicom_files', 'in_files')]),
            (series_spec, store_dicom, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
            (ctp_dir, compress, [('out_dir', 'dest')]),
            (fix_dicom, compress, [('out_files', 'in_file')]),
            (compress, store_dicom, [('out_file', 'in_files')])]
    
    def _create_stack(self, series_spec, dcm_files_node):
        """
        Creates the series stack connections.
        
        @param series_spec: the series specification node
        @param dcm_files_node: the series input files node
        @return: the stack workflow connections
        """
        
        # Stack the series.
        stack = pe.Node(DcmStack(embed_meta=True, out_format="series%(SeriesNumber)03d"),
            name='stack')
        
        # Store the series stack in XNAT.
        store_stack=pe.Node(XNATUpload(project='QIN'), name='store_stack')
        
        return [
            (series_spec, store_stack, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
            (dcm_files_node, stack, [('dicom_files', 'dicom_files')]),
            (stack, store_stack, [('out_file', 'in_files')])]

def _ctp_series_directory(dest, subject, session, series):
    """
    @return: the dest/subject/session/series directory path
    """
    import os
    return os.path.join(dest, subject, session, str(series))
