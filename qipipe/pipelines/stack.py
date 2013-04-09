import nipype.pipeline.engine as pe
from nipype.interfaces.utility import Function
from nipype.interfaces.dcmstack import DcmStack
from ..interfaces import XNATUpload

def create_stack_connections(series_spec, input_node, input_field):
    """
    Creates the series stack connections.
    
    @param series_spec: the series specification node
    @param input_node: the DICOM files input node
    @param input_field: the DICOM files input field name
    @return: the stack workflow connections
    """
    
    # Stack the series.
    stack = pe.Node(DcmStack(embed_meta=True, out_format="series%(SeriesNumber)03d"),
        name='stack')
    
    # Store the series stack in XNAT.
    store_stack=pe.Node(XNATUpload(project='QIN'), name='store_stack')
    
    return [
        (series_spec, store_stack, [('subject', 'subject'), ('session', 'session'), ('series', 'scan')]),
        (input_node, stack, [(input_field, 'dicom_files')]),
        (stack, store_stack, [('out_file', 'in_files')])]
