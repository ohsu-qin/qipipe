import os, glob
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.io import DataSink
from nipype.interfaces.dcmstack import DcmStack
from .pipeline_helper import uncompress as ucmpfunc
from .pipeline_helper import pvs_substitution as pvsfunc
from .pipeline_helper import PVS_KEYS

__all__ = ['run', 'store']

def run(collection, *series_dirs):
    """
    Imports the given series directories into XNAT.
    
    @param collection: the CTP image collection (C{Breast} or C{Sarcoma})
    @param series_dirs: the staged series directories
    """
    wf.inputs.infosource.collection = collection
    wf.get_node('infosource').iterables = ('series_dir', series_dirs)
    wf.run()

def _run(series_dir):
    from qipipe.pipelines.xnat import run
    
    run(series_dir)

store = Function(input_names=['collection', 'series_dir'], output_names=[], function=_run)
"""The XNAT import pipeline Function."""

# The XNAT import pipeline loads the staged DICOM files as series NiFTI stacks into XNAT.
wf = pe.Workflow(name='xnat')

# The XNAT import workflow facade node.
infosource = pe.Node(interface=IdentityInterface(fields=['collection', 'series_dir']), name='infosource')

# TODO - finder series -> dcm.gz files
s2dcm = ...

# The DICOM file uncompressor. The uncompress input is a compressed DICOM file.
# The uncompress output is the uncompressed file in the local run context data
# subdirectory. Since the fix
# output is a list of files, the fix output is multiplexed into a compress MapNode.
# The MapNode output is a list of files.
uncompress = pe.MapNode(ucmpfunc, name='uncompress', iterfield=['in_file'])
uncompress.inputs.dest = 'data'

# Stack each DICOM series as a single NIfTI file.
stack = pe.Node(interface=DcmStack(), name='stack')
stack.inputs.embed_meta = True
stack.inputs.out_format = "series%(SeriesNumber)03d"

# The patient/visit/series substitutions factory. The pvs input is a patient directory
# path. The pvs output is a substitutions assignment as described in the pvs_substitution
# helper Function.
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
    (fix, stack, [('out_files', 'dicom_files')]),
    (pvs, assemble, [('substitutions', 'substitutions')]),
    (compress, assemble, [('out_file', ASSEMBLY_FLD)])])
