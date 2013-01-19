import nipype.pipeline.engine as pe
from .interfaces.fix_dicom import FixDicom
from nipype.interfaces.fsl.preprocess import ApplyWarp


pipeline = pe.Workflow(name='qipipe')
fix_dicom = pe.Node(interface=FixDicom(), name='fix_dicom')
pipeline.add_nodes([fix_dicom])
