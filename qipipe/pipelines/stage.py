import os, glob
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.io import DataSink
from qipipe.staging.group_dicom import group_dicom_files
from qipipe.interfaces import FixDicom
from .pipeline_helper import compress as cmpfunc
from .pipeline_helper import pvs_substitution as pvsfunc
from .pipeline_helper import PVS_KEYS

__all__ = ['run', 'stage']

def run(collection, dest, *patient_dirs):
    """
    Stages the given AIRC patient directories for import into CTP.
    The destination directory is populated with two subdirectories as follows:
        - airc: the patient/visit/series hierarchy linked to the AIRC source
        - ctp: the CTP import staging area
    
    The C{airc} area contains links to the AIRC study DICOM files.
    The staging area contains compressed DICOM files suitable for import into
    CTP. The DICOM headers are modified to correct the C{Patient ID} and
    C{Body Part Examined} tags.
    
    @param collection: the CTP image collection (C{Breast} or C{Sarcoma})
    @param dest: the destination directory
    @param patient_dirs: the AIRC source patient directories to stage
    """
    wf.inputs.infosource.collection = collection
    wf.inputs.infosource.ctp = os.path.join(dest, 'ctp')
    airc = os.path.join(dest, 'airc')
    # group_dicom_files is not included as a workflow node because nipype
    # structural constraints are too unwieldly, specifically:
    # 1) The group output might be empty, and nipype unconditionally executes
    #    all nodes, regardless of whether there are inputs.
    # 2) The group input is a patient directory and the output is the list of
    #    new series, which is input to the successor nodes. nipype cannot handle
    #    this structural mismatch without resorting to obscure kludges.
    dirs = group_dicom_files(*patient_dirs, dest=airc, include='*concat*/*')
    if dirs:
        wf.get_node('infosource').iterables = ('series_dir', dirs)
        wf.run()

def _run(collection, dest, patient_dir):
    from qipipe.pipelines.stage import run
    
    run(collection, dest, patient_dir)

stage = Function(input_names=['collection', 'dest', 'patient_dir'], output_names=[], function=_run)
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

# The patient/visit/series substitutions factory. The pvs input is a path. The pvs
# output is a substitutions assignment as described in the pvs_substitution helper
# Function.
pvs = pe.Node(pvsfunc, name='pvs')

# The result copier. The input field specifies the patient/visit/series hierarchy.
# The parameterization flag prevents the DataSink from injecting an extraneous directory,
# e.g. _compress0, into the target location.
ASSEMBLY_FLD = '.'.join(PVS_KEYS) + '.@file'
assemble = pe.Node(interface=DataSink(infields=[ASSEMBLY_FLD], parameterization=False), name='assemble')

# Build the pipeline.
wf.connect([
    (infosource, pvs, [('series_dir', 'path')]),
    (infosource, fix, [('collection', 'collection'), ('series_dir', 'source')]),
    (infosource, assemble, [('ctp', 'base_directory')]),
    (fix, compress, [('out_files', 'in_file')]),
    (pvs, assemble, [('substitutions', 'substitutions')]),
    (compress, assemble, [('out_file', ASSEMBLY_FLD)])])