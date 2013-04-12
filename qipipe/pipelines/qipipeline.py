"""The qipipeline L{run} function is the OHSU QIN pipeline facade."""

import os
from . import staging
from . import registration
from . import pk_mapping

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
        - Registers each new visit.
        - Uploads the resampled images into XNAT.
        - Performs a parameteric mapping on both the scanned and resampled images.
        - Uploads the parameteric mappings into XNAT.
    
    The supported AIRC collections are defined L{qipipe.staging.airc_collection}.

    The options include the workflows to run, as well as any additional
    L{QIPipeline.run} options.
    
    The destination directory is populated with the CTP import staging files.
    
    @param collection: the AIRC image collection name
    @param dest: the destination directory
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: additional workflow options
    """

    workflows = opts.pop('workflows', [])
    qip = QIPipeline(collection, *workflows)
    return qip.run(*subject_dirs, **opts)
    
class QIPipeline(object):
    """The OHSU QIN pipeline."""
    
    def __init__(self, collection):
        """
        @param collection: the AIRC image collection name
        """
        self.collection = collection
    
    def run(self, *subject_dirs, **opts):
        """
        Runs this pipeline on the the given AIRC subject directories.
        
        @param subject_dirs: the AIRC source subject directories to stage
        @param opts: the pipeline options
        @keyword dest: the destination directory (default current working directory)
        @keyword work: the pipeline execution work area (default a new temp directory)
        @return: the new XNAT session labels
        """
        
        # The work option is the pipeline parent directory.
        work_dir = opts.pop('work', tempfile.mkdtemp())
        
        stg_dir = os.path.join(work_dir, 'stage')
        sessions = staging.run(base_dir=stg_dir, *subject_dirs, **opts)
        
        return sessions
