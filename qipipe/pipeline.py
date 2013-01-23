import nipype.pipeline.engine as pe
from .interfaces.fix_dicom import FixDicom
from nipype.interfaces.dcmstack import DcmStack
from nipype.interfaces.fsl.preprocess import ApplyWarp


pipeline = pe.Workflow(name='qipipe')
fix_dicom = pe.Node(interface=FixDicom(), name='fix_dicom')
stacker = pe.Node(interface=DcmStack(), name='stack')
stacker.inputs.out_format = "series%(SeriesNumber)03d"
pipeline.connect(fix_dicom, 'dest', stacker, 'dicom_files')
