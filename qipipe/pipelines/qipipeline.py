"""The qipipeline :meth:`run` function is the OHSU QIN pipeline facade."""

import os, tempfile
from ..helpers import xnat_helper
from .pipeline_error import PipelineError
from . import staging
from . import registration
from . import modeling

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

    - Masks, registers and reslices each new visit.

    - Uploads the resliced images into XNAT.

    - Performs a parameteric mapping on both the scanned and resliced images.

    - Uploads the parameteric mappings into XNAT.
    
    The supported AIRC collections are defined in :mod:`qipipe.staging.airc_collection`.
    
    The options include the workflows to run, as well as any additional
    :meth:`QIPipeline.run` options.
    
    The destination directory is populated with the CTP import staging files.
    
    :param collection: the AIRC image collection name
    :param dest: the destination directory
    :param subject_dirs: the AIRC source subject directories to stage
    :param opts: additional workflow options
    """
    
    qip = QIPipeline(collection)
    return qip.run(*subject_dirs, **opts)


class QIPipeline(object):
    """The OHSU QIN pipeline."""
    
    def __init__(self, collection):
        """
        :param collection: the AIRC image collection name
        """
        self.collection = collection
    
    def run(self, *subject_dirs, **opts):
        """
        Runs this pipeline on the the given AIRC subject directories.
        The OHSU QIN pipeline consists of three workflows:

        - staging: Prepare the new AIRC DICOM visit

        - registration: Mask, register and reslice the staged images

        - modeling: Calculate the PK parameters
        
        The default options for each of these constituent workflows can be overridden
        by setting the ``staging``, ``registration`` or ``modeling`` option, resp.
        If the ``registration`` option is set to false, then only staging is performed.
        If the ``modeling`` option is set to false, then modeling is not performed.
        
        The resliced XNAT (subject, session, reconstruction) designator tuples
        
        :param subject_dirs: the AIRC source subject directories to stage
        :param opts: the pipeline options
        :keyword dest: the CTP staging destination directory (default current working directory)
        :keyword work: the pipeline execution work area (default a new temp directory)
        :return: the pipeline result
        """
        # The work option is the pipeline parent directory.
        work_dir = opts.pop('work', None) or tempfile.mkdtemp()
        
        with xnat_helper.connection():
            # Stage the input AIRC files.
            stg_dir = os.path.join(work_dir, 'stage')
            session_specs = staging.run(self.collection, *subject_dirs, base_dir=stg_dir, **opts)
            if not session_specs:
                return []
            
            # The dest option is only used for staging.
            opts.pop('dest', None)
            
            # If the register flag is set to False, then return the staged XNAT sessions.
            if opts.get('registration') == False:
                logger.debug("Skipping registration since the registration option is set to False.")
                return session_specs
            
            reg_dir = os.path.join(work_dir, 'register')
            reg_specs = registration.run(*session_specs, base_dir=reg_dir, **opts)
            
            # If the modeling flag is set to False, then return the registered XNAT reconstructions.
            if opts.get('modeling') == False:
                logger.debug("Skipping modeling since the modeling option is set to False.")
                return reg_specs
            
            mdl_dir = os.path.join(work_dir, 'modeling')
            mdl_inputs = [dict(subject=sbj, session=sess, reconstruction=recon) for sbj, sess, recon in reg_specs]
            return modeling.run(*mdl_inputs, base_dir=mdl_dir, **opts)
