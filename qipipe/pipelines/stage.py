import os, glob, shutil
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.io import DataFinder, DataSink
from qipipe.staging.group_dicom import group_dicom_files
from qipipe.staging.ctp import create_ctp_id_map
from qipipe.interfaces import FixDicom
from .pipeline_helper import pvs
from .pipeline_helper import compress as cmpfunc
__all__ = ['run', 'stage']

CTP_ID_MAP = 'QIN-SARCOMA-OHSU.ID-LOOKUP.properties'
"""The id map file name specified by CTP."""

CTP_COLLECTION_DICT = dict(Breast='QIN-BREAST-02', Sarcoma='QIN-SARCOMA-01')

def run(collection, dest, *subject_dirs, **opts):
    """
    Stages the given AIRC subject directories for import into CTP.
    The destination directory is populated with two subdirectories as follows:
        - airc: the subject/session/series hierarchy linked to the AIRC source
        - ctp: the CTP import staging area
    
    The C{airc} subdirectory contains links to the AIRC study DICOM files.
    
    The C{ctp} subdirectory contains compressed DICOM files suitable for
    import into CTP. The DICOM headers are modified to correct the C{Patient ID}
    and C{Body Part Examined} tags. The CTP id map file L{CTP_ID_MAP} is
    created is created in the C{ctp} subdirectory as well.
    
    @param collection: the CTP image collection (C{Breast} or C{Sarcoma})
    @param dest: the destination directory
    @param subject_dirs: the AIRC source subject directories to stage
    @param opts: additional L{group_dicom_files} options
    """
    wf.inputs.infosource.collection = collection
    dest = os.path.abspath(dest)
    ctp_dir = os.path.join(dest, 'ctp')
    wf.inputs.infosource.ctp = ctp_dir
    airc = os.path.join(dest, 'airc')
    # The group options.
    gopts = dict(dest=airc, dicom_pat='*concat*/*')
    gopts.update(opts)
    # group_dicom_files is not included as a workflow node because nipype
    # structural constraints are too unwieldly, specifically:
    # 1) The group output might be empty, and nipype unconditionally executes
    #    all nodes, regardless of whether there are inputs.
    # 2) The group input is a subject directory and the output is the list of
    #    new series, which is input to the successor nodes. nipype cannot handle
    #    this structural mismatch without resorting to obscure kludges.
    dirs = group_dicom_files(*subject_dirs, **gopts)
    if dirs:
        # Iterate over each series directory.
        wf.get_node('infosource').iterables = ('series_dir', dirs)
        # Run the pipeline.
        wf.run()
        # The subject directories to include in the CTP id mapping file.
        sbj_dirs = glob.glob(os.path.join(ctp_dir, 'subject*'))
        # The the CTP id mapping file output stream.
        output = open(os.path.join(ctp_dir, CTP_ID_MAP), 'w+')
        # Write the id map.
        ctp_coll = CTP_COLLECTION_DICT[collection]
        create_ctp_id_map(ctp_coll, first_only=True, *sbj_dirs).write(output)

class Stage(object):
    def __init__(collection, dest, **opts):
        """
        @param collection: the CTP image collection (C{Breast} or C{Sarcoma})
        @param dest: the destination directory
        @param opts: additional L{group_dicom_files} options
        """
        self.collection = collection
        self.dest = os.path.abspath(dest)
        self.ctp_dir = os.path.join(dest, 'ctp')
        self.airc_dir = os.path.join(dest, 'airc')
        # The group options.
        self.opts = dict(dest=airc_dir, dicom_pat='*concat*/*')
        self.opts.update(opts)
        # The memory context.
        self.mem = Memory(tempfile.mkdtemp())
        
    
    def stage(*subject_dirs):
        """
        Stages the given AIRC subject directories for import into CTP.
        The destination directory is populated with two subdirectories as follows:
            - airc: the subject/session/series hierarchy linked to the AIRC source
            - ctp: the CTP import staging area

        The C{airc} subdirectory contains links to the AIRC study DICOM files.

        The C{ctp} subdirectory contains compressed DICOM files suitable for
        import into CTP. The DICOM headers are modified to correct the C{Patient ID}
        and C{Body Part Examined} tags. The CTP id map file L{CTP_ID_MAP} is
        created is created in the C{ctp} subdirectory as well.

        @param subject_dir: the AIRC source subject directories to stage
        """
        fix = mem.cache(FixDicom)(dest=self.ctp_dir)
        
        
        
        
        
        
        wf.inputs.infosource.collection = collection
        dest = os.path.abspath(dest)
        ctp_dir = os.path.join(dest, 'ctp')
        wf.inputs.infosource.ctp = ctp_dir
        airc = os.path.join(dest, 'airc')
        # The group options.
        gopts = dict(dest=airc, dicom_pat='*concat*/*')
        gopts.update(opts)
        # group_dicom_files is not included as a workflow node because nipype
        # structural constraints are too unwieldly, specifically:
        # 1) The group output might be empty, and nipype unconditionally executes
        #    all nodes, regardless of whether there are inputs.
        # 2) The group input is a subject directory and the output is the list of
        #    new series, which is input to the successor nodes. nipype cannot handle
        #    this structural mismatch without resorting to obscure kludges.
        dirs = group_dicom_files(*subject_dirs, **gopts)
        if dirs:
            # Iterate over each series directory.
            wf.get_node('infosource').iterables = ('series_dir', dirs)
            # Run the pipeline.
            wf.run()
            # The subject directories to include in the CTP id mapping file.
            sbj_dirs = glob.glob(os.path.join(ctp_dir, 'subject*'))
            # The the CTP id mapping file output stream.
            output = open(os.path.join(ctp_dir, CTP_ID_MAP), 'w+')
            # Write the id map.
            create_ctp_id_map(collection, first_only=True, *sbj_dirs).write(output)
    
def _stage(series_dirs):
mem = Memory('/tmp')



mem.clear_previous_runs()


def _run(collection, dest, subject_dir):
    from qipipe.pipelines.stage import run
    
    run(collection, dest, subject_dir)

stage = Function(input_names=['collection', 'dest', 'subject_dir'], output_names=[], function=_run)
"""The staging pipeline Function."""

# The staging pipeline prepares the AIRC image files for submission to CTP.
wf = pe.Workflow(name='stage')

# The stage workflow facade node.
infosource = pe.Node(interface=IdentityInterface(fields=['collection', 'ctp', 'series_dir']), name='infosource')

# Fix the AIRC DICOM tags. The fix input is a series directory. The fix output is
# the modified DICOM files.
fix = pe.Node(interface=FixDicom(dest='data'), name='fix')

# The DICOM file compressor. The compress input is a DICOM file. The compress output
# is the compressed file in the local run context data subdirectory. Since the fix
# output is a list of files, the fix output is multiplexed into a compress MapNode.
# The MapNode output is a list of files.
compress = pe.MapNode(cmpfunc, name='compress', iterfield=['in_file'])
compress.inputs.dest = 'data'

# The subject/session/series substitution.
# The inputs are the subject, session and series singleton lists matched on a series directory.
# The output is a substitutions assignment.
def pvs_substitution(subject, session, series):
    return dict(_subject=subject, _session=session, _series=series).items()
pvsfunc = Function(input_names=['subject', 'session', 'series'], output_names=['substitutions'],
    function=pvs_substitution)
pvs_substitution = pe.Node(pvsfunc, name='pvs_substitution')

# The result copier. The input field specifies the subject/session/series hierarchy.
# Unsetting the parameterization flag prevents the DataSink from injecting an extraneous directory,
# e.g. _compress0, into the target location.
ASSEMBLY_FLD = '_subject._session._series.@file'
assemble = pe.Node(interface=DataSink(infields=[ASSEMBLY_FLD], parameterization=False), name='assemble')

# Build the pipeline.
wf.connect([
    (infosource, pvs, [('series_dir', 'root_paths')]),
    (infosource, fix, [('collection', 'collection'), ('series_dir', 'source')]),
    (infosource, assemble, [('ctp', 'base_directory')]),
    (fix, compress, [('out_files', 'in_file')]),
    (pvs, pvs_substitution, [('subject', 'subject'), ('session', 'session'), ('series', 'series')]),
    (pvs_substitution, assemble, [('substitutions', 'substitutions')]),
    (compress, assemble, [('out_file', ASSEMBLY_FLD)])])
