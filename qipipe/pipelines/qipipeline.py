import os, glob, shutil
import nipype.pipeline.engine as pe
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.io import DataFinder, DataGrabber, XNATSink
from nipype.interfaces.dcmstack import DcmStack
from nipype.interfaces.fsl.preprocess import ApplyWarp
from qipipe.pipelines.stage import stage as stgfunc
from .pipeline_helper import uncompress as ucfunc
from .xnat import XNAT_CFG, subject_id_for_label

import logging
logger = logging.getLogger(__name__)

__all__ = ['run', 'qipipeline']

def run(collection, dest, *subject_dirs):
    """
    Runs the qipipeline on the the given AIRC subject directories as follows:
        - Stages the input for import into CTP using L{qipipe.pipelines.stage.run}.
        - Stacks each new series as a NiFTI file using DcmStack.
        - Imports each new series stack into XNAT.
        - Registers each new series.
        - Imports each new registered series into XNAT.
    
    @param collection: the CTP image collection (C{Breast} or C{Sarcoma})
    @param dest: the destination directory
    @param subject_dirs: the AIRC source subject directories to stage
    """
    # Stage the new series images.
    #stage.run(collection, dest, *subject_dirs)
    # The stage output directory containing the new series DICOM files.
    ctp_dir = os.path.join(dest, 'ctp')
    # Run the workflow on each new series.
    for sbj_dir in glob.glob(ctp_dir + '/subject*'):
        pt_nbr = sbj_dir[-2:]
        wf.inputs.infosource.subject = collection + pt_nbr
        series_dirs = glob.glob(os.path.join(ctp_dir, sbj_dir) + '/session*/series*')
        wf.get_node('infosource').iterables = [('series_dir', series_dirs)]
        wf.run()

def _run(collection, dest, subject_dir):
    from qipipe.pipelines.qipipeline import run
    
    run(collection, dest, subject_dir)

qipipeline = Function(input_names=['collection', 'dest', 'subject_dir'], output_names=[], function=_run)
"""The QI pipeline Function."""

# The QI pipeline.
wf = pe.Workflow(name='qipipeline')

# The qipipeline workflow facade node.
infosource = pe.Node(interface=IdentityInterface(fields=['subject', 'series_dir']), name='infosource')

# The images within the series.
dicom = pe.Node(interface=DataGrabber(outfields=['images']), name='dicom')
dicom.inputs.template = '*.dcm.gz'

# The DICOM file uncompressor. The uncompress input is a compressed DICOM file. The
# compress output is the uncompressed file in the local run context data subdirectory.
uncompress = pe.MapNode(ucfunc, name='uncompress', iterfield=['in_file'])
uncompress.inputs.dest = 'data'

# Stack each series as a single NIfTI file.
stack = pe.Node(interface=DcmStack(), name='stack')
stack.inputs.embed_meta = True
stack.inputs.out_format = "series%(SeriesNumber)03d"

# The subject id is the XNAT subject label. Convert it to the XNAT subject id.
label2id = pe.Node(subject_id_for_label, name = 'label2id')
label2id.inputs.project = 'QIN'

# Import the NiFTI series stacks into XNAT.
xnat = pe.Node(interface=XNATSink(input_names=['series_stack']), name='xnat')
xnat.inputs.config = XNAT_CFG
xnat.inputs.project_id = 'QIN'
xnat.inputs.experiment_id = 'qipipeline'
xnat.inputs.share = True

# Register the images.

# Build the pipeline.
wf.connect([(infosource, dicom, [('series_dir', 'base_directory')]),
    (dicom, uncompress, [('images', 'in_file')]),
    (uncompress, stack, [('out_file', 'dicom_files')]),
    (infosource, label2id, [('subject', 'label')]),
    (label2id, xnat, [('subject_id', 'subject_id')]),
    (stack, xnat, [('out_file', 'series_stack')])])
