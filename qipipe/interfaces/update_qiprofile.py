import re
from nipype.interfaces.base import (traits, BaseInterfaceInputSpec,
                                    BaseInterface)
from ..helpers.qiprofile_helper import QIProfile


class UpdateQIProfileInputSpec(BaseInterfaceInputSpec):
    project = traits.Str(mandatory=True, desc='The XNAT project id')

    subject = traits.Str(mandatory=True, desc='The XNAT subject name')

    session = traits.Str(mandatory=True, desc='The XNAT session name')

    session_params = traits.Dict({}, usedefault=True,
                                 desc='The imaging acquistion parameters'
                                      ' name-value dictionary')

    assessor = traits.Str(desc='The XNAT modeling assessor name')

    modeling_params = traits.Dict({}, usedefault=True,
                                  desc='The PK modeling parameters'
                                       ' name-value dictionary')


class UpdateQIProfile(BaseInterface):
    """
    The ``UpdateQIProfile`` Nipype interface updates the Imaging Profile
    database.
    """

    input_spec = UpdateQIProfileInputSpec

    def _run_interface(self, runtime):
        db = QIProfile()
        sess = db.save_session(self.inputs.project, self.inputs.subject,
                               self.inputs.session, **self.inputs.session_params)
        if self.inputs.assessor:
            mdl = Modeling(session=sess, name=self.inputs.assessor,
                           **self.inputs.modeling_params)
            mdl.save()

        return runtime