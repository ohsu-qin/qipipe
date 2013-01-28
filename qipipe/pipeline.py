import nipype.pipeline.engine as pe
from .interfaces.group_dicom import GroupDicom
from .interfaces.fix_dicom import FixDicom
from nipype.interfaces.io import DataGrabber
from nipype.interfaces.dcmstack import DcmStack
from nipype.interfaces.fsl.preprocess import ApplyWarp


# The master pipeline.
pipeline = pe.Workflow(name='qipipe')

# Group the AIRC DICOM input files by series.
group = pe.Node(interface=GroupDicom(), name='group', dest='data')

# Fix the AIRC DICOM tags.
fix = pe.Node(interface=FixDicom(), name='fix', dest='data')

# TODO - in/pt/visit* +-> group/data/series* +-> fix/data -> stack/seriesnnnn.nii.gz -> datasink base_directory/(container = visitnn)
# Use Mapper for in -> ( ... ) -> datasink ?

# Grab the patient series subdirectories.
p2v = pe.Node(interface=DataGrabber(), name='p2v')
p2v.inputs.template = '*'
p2v.inputs.field_template = dict(outfiles='visit*')

# Grab the patient series subdirectories.
v2s = pe.Node(interface=DataGrabber(), name='v2s')
v2s.inputs.template = '*'
v2s.inputs.field_template = dict(outfiles='series*')

# Stack each series as a single NIfTI file.
stack = pe.Node(interface=DcmStack(), name='stack')
stack.inputs.out_format = "series%(SeriesNumber)03d"

# Copy each series stack to the destination.
assemble = pe.Node(nio.DataSink(), name='assemble')

# Build the pipeline.
pipeline.connect([
    (group, fix, [('target', 'source')]),
    (fix, p2v, [('target', 'base_directory')]),
    (p2v, p2s, [('outfiles', 'base_directory')]),
    (p2s, stack, [('outfiles', 'dicom_files')])
    (stack, assemble [('out_file', '@')])
    ])
