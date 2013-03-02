import os, glob
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.io import DataFinder, XNATSink
from nipype.interfaces.dcmstack import DcmStack
from .pipeline_helper import pvs
from .pipeline_helper import uncompress as ucmpfunc
from ..helpers.xnat_helper import XNAT
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

def _run(collection, series_dir):
    from qipipe.pipelines.xnat import run
    
    run(collection, series_dir)

store = Function(input_names=['collection', 'series_dir'], output_names=[], function=_run)
"""The XNAT import pipeline Function."""

# The XNAT import pipeline loads the staged DICOM files as series NiFTI stacks into XNAT.
wf = pe.Workflow(name='xnat')

# The XNAT import workflow facade node.
infosource = pe.Node(interface=IdentityInterface(fields=['collection', 'series_dir']), name='infosource')

# The compressed image files.
dcmgz_finder = pe.Node(DataFinder(match_regex='.+/.*\.dcm\.gz'), name='dcmgz_finder')

# The DICOM file uncompressor. The uncompress input is a compressed DICOM file.
# The uncompress output is the uncompressed file in the local run context data
# subdirectory. Since both the predecessor output and successor input are file
# lists, this uncompressor node is a MapNode.
uncompress = pe.MapNode(ucmpfunc, name='uncompress', iterfield=['in_file'])
uncompress.inputs.dest = 'data'

# Stack each DICOM series as a single NIfTI file.
stack = pe.Node(interface=DcmStack(), name='stack')
stack.inputs.embed_meta = True
stack.inputs.out_format = "series%(SeriesNumber)03d"

def stack_series_number(path):
    """
    Extracts the series number from the stack file name.
    
    @param path: the series stack file path
    @return: the series number
    """
    import os, re
    _, fname = os.path.split(path)
    return int(re.search('series(\d{3})', fname).group(1))

ssnfunc = Function(input_names=['path'], output_names=['series'],
    function=stack_series_number)

stack_series_nbr = pe.Node(ssnfunc, name='stack_series_nbr')

def patient2subject(collection, patient):
    """
    Makes the XNAT subject label.
    
    @param collection: the collection name
    @param patient: the patient name, ending in the two-digit patient number
    @return: the collection name concatenated with the patient number
    """
    return collection + patient[-2:]

pt2sbjfunc = Function(input_names=['collection', 'patient'], output_names=['subject'],
    function=patient2subject)

pt2sbj = pe.Node(pt2sbjfunc, name='pt2sbj')

def visit2session(subject, visit):
    """
    Makes the XNAT session label.
    
    @param subject: the subject name
    @param visit: the visit name
    @return: the subject and visit joined by an underscore
    """
    return '%s_%s' % (subject, 'Session' + visit[-2:])

v2sfunc = Function(input_names=['subject', 'visit'], output_names=['session'],
    function=visit2session)

visit2session = pe.Node(v2sfunc, name='visit2session')

def upload_image_file(subject, session, series, path):
    """Stores the image file in XNAT."""
    from qipipe.helpers.xnat_helper import XNAT
    XNAT().upload('QIN', subject, session, path, scan=series, modality='MR')

uploadfunc = Function(input_names=['subject', 'session', 'path', 'series'],
    output_names=[],
    function=upload_image_file)

upload_series_stack = pe.Node(uploadfunc, name='upload_series_stack')

# Build the pipeline.
wf.connect([
    (infosource, dcmgz_finder, [('series_dir', 'root_paths')]),
    (infosource, pt2sbj, [('collection', 'collection')]),
    (infosource, pvs, [('series_dir', 'root_paths')]),
    (pvs, pt2sbj, [('patient', 'patient')]),
    (pvs, visit2session, [('visit', 'visit')]),
    (pt2sbj, visit2session, [('subject', 'subject')]),
    (visit2session, upload_series_stack, [('session', 'session')]),
    (pt2sbj, upload_series_stack, [('subject', 'subject')]),
    (dcmgz_finder, uncompress, [('out_paths', 'in_file')]),
    (uncompress, stack, [('out_file', 'dicom_files')]),
    (stack, stack_series_nbr, [('out_file', 'path')]),
    (stack_series_nbr, upload_series_stack, [('series', 'series')]),
    (stack, upload_series_stack, [('out_file', 'path')])])
