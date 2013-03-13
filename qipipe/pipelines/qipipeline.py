"""The qipipeline L{run} function is the OHSU QIN pipeline facade."""

import os, glob
from nipype.caching import Memory
from .group_dicom import GroupDicom

def run(collection, dest, *subject_dirs, **opts):
    """
    Runs the qipipeline on the the given AIRC subject directories as follows:
        - Groups the input DICOM images into series.
        - Stages the series for import into CTP.
        - Makes the CTP subject id map.
        - Stacks each new series as a NiFTI file using DcmStack.
        - Imports each new series stack into XNAT.
        - Registers each new series.
        - Imports each new registered series into XNAT.

    The destination directory is populated with two subdirectories as follows:
        - airc: the subject/session/series hierarchy linked to the AIRC source
        - ctp: the CTP import staging area

    The C{airc} subdirectory contains links to the AIRC study DICOM files as
    described in L{GroupDicom}.

    The C{ctp} subdirectory contains compressed DICOM files suitable for
    import into CTP as described in L{StageDicom}.
        
    Supported pipeline options include the following:
        - cache_dir: the pipeline cache location (default is the current directory)
    
    @param collection: the CTP image collection (C{Breast} or C{Sarcoma})
    @param dest: the destination directory
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: the pipeline options
    """
    # Create a memory context.
    if opts.has_key('cache_dir'):
        cache_dir = opts['cache_dir']
    else:
        cache_dir = '.'
    mem = Memory(cache_dir)

    # The output locations are destination subdirectories.
    dest = os.path.abspath(dest)
    airc_dir = os.path.join(dest, 'airc')
    ctp_dir = os.path.join(dest, 'ctp')

    # Group the AIRC input into series directories.
    grp_result = mem.cache(GroupDicom)(ollection=collection, dest=airc_dir, subject_dirs=subject_dirs)
    
    # Stage the new series images.
    staged = mem.cache(StageCTP)(series_dirs=grp_result.outputs['series_dirs'])
    
    # The XNAT uploader.
    xnat = mem.Cache(XNATUpload)
    
    # Stack each series as a single NIfTI file.
    stacker = mem.Cache(DcmStack)
    for d in staged:
        # The subject, session and series directory components.
        sbj, sess, ser = match_series_hierarchy(d)
        # Stack the staged files.
        stacked = stacker(embed_meta=True, out_format="series%(SeriesNumber)03d", dicom_files=d)
        # Store the stacked series in XNAT.
        xnat('QIN', sbj, sess, stacked, scan=ser, modality='MR')
