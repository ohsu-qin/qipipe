import re
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    BaseInterface)
from ..helpers import qiprofile


class UpdateQIProfileInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')
    
    subject = traits.Str(mandatory=True, desc='The XNAT subject name')
    
    session = traits.Str(mandatory=True, desc='The XNAT session name')


class UpdateQIProfile(BaseInterface):
    """
    The ``UpdateQIProfile`` Nipype interface updates the Imaging Profile
    database.
    """
    
    input_spec = UpdateQIProfileInputSpec
    
    def _run_interface(self, runtime):
        qiprofile.update_session(self.inputs.project, self.inputs.subject,
                               self.inputs.session)
        return runtime
