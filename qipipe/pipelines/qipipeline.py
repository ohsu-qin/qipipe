"""The qipipeline L{run} function is the OHSU QIN pipeline facade."""

import os
import nipype.pipeline.engine as pe
from ..interfaces import Glue
from . import staging
from . import stack

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

    The options include the components to run, as well as any additional
    L{QIPipeline.run} options.
    
    The destination directory is populated with the CTP import staging files.
    
    @param collection: the AIRC image collection name
    @param dest: the destination directory
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: additional workflow options
    @keyword components: the L{QIPipeline.COMPONENTS} to run (default all)
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
        @param collection: the  AIRC image collection name
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
        
        @param dest: the CTP staging location
        @param subject_dirs: the AIRC source subject directories to stage
        @param opts: additional workflow options
        @keyword dest: the destination directory (default current working directory)
        @keyword work: the pipeline execution work area (default a new temp directory)
        @return: the new XNAT session labels
        """

        # Collect the new AIRC visit series specifications. Each series spec is a
        # (subject, session, series, dicom files) tuple.
        ser_specs = staging.new_series_specs(self.collection, *subject_dirs)

        # The work option is the workflow base directory.
        if 'work' in opts:
            opts['base_dir'] = opts.pop('work')
        
        # The staging location.
        dest = os.path.abspath(opts.pop('dest', os.getcwd()))
        
        # Make the workflow.
        wf = self._create_workflow(*ser_specs, **opts)
        
        # Set the top-level workflow input destination.
        wf.inputs.input_spec.dest = dest
        
        # Diagram the workflow graph.
        grf = os.path.join(wf.base_dir, 'qipipeline.dot')
        wf.write_graph(dotfilename=grf)
        logger.debug("The workflow graph is depicted at %s.png." % grf)
        
        # Run the workflow.
        wf.run()
        
        # Return all new sessions.
        sessions = {sess for _, sess, _, _ in ser_specs}
        return sessions
    
    def _create_workflow(self, *series_specs, **opts):
        """
        Builds the workflow for the given series. The workflow is built as part of
        the L{run} method.
        
        @param series_specs: the (subject, session, series, dicom files) inputs
        @param opts: nipype workflow options
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
        wf.connect(staging.create_staging_connections(self.collection, series_spec))
        
        # Set the top-level workflow input collection and subjects.
        wf.inputs.input_spec.collection = self.collection
        subjects = list({sbj for sbj, _, _, _ in series_specs})
        wf.inputs.input_spec.subjects = subjects

        # Pipe the fix_dicom staging node to the stack input.
        if QIPipeline.STACK in self.components:
            fix_dicom = wf.get_node('fix_dicom')
            wf.connect(stack.create_stack_connections(series_spec, fix_dicom, 'out_files'))
        
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
        tuple. The node output is the disaggregated subject, session, series and dicom_files fields.
        This node iterates over each input series spec tuple.
        
        @param series_specs: the (subject, session, series, dicom files) inputs
        @return: the series spec node
        """
        glue = Glue(input_names=['series_spec'], output_names=['subject', 'session', 'series', 'dicom_files'])
        node = pe.Node(glue, name='series_spec')
        node.iterables = ('series_spec', series_specs)
        return node
