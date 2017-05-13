import re
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    BaseInterface)
from ..qiprofile.update import update


class UpdateQIProfileInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    collection = traits.Str(mandatory=True, desc='The subject collection')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(mandatory=True, desc='The XNAT session name')

    in_file = traits.Str(mandatory=True, desc='The clinical spreadsheet file')


class UpdateQIProfile(BaseInterface):
    """
    The ``UpdateQIProfile`` Nipype interface updates the Imaging Profile
    database.
    """

    input_spec = UpdateQIProfileInputSpec
    
    def _run_interface(self, runtime):
        update(self.inputs.project, self.inputs.collection,
               self.inputs.subject, self.inputs.session,
               self.inputs.in_file)

        return runtime
