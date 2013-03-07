"""
Groups AIRC input DICOM files by series.
The AIRC directory structure is required to be in the form:
    
    I{subject}/I{session}/I{dicom}

where I{subject}, I{session}, I{dicom} are the respective path glob
patterns, as described in L{group_dicom_files}. The output is the
grouped series directories in the form:

    C{subject}NN/C{session}NN/C{series}NNN

where the series directory consists of links to the AIRC input
DICOM files.

Examples:

For input DICOM files with directory structure:

    C{BreastChemo4/}
        CVisit1/}
            C{dce_concat/}
                C{120210 ex B17_TT49}
                ...

    GroupDicom(subject_dirs=[BreastChemo4], session_pat='Visit*', dicom_pat='*concat*/*', dest=data).run()

results in output links with directory structure:

    C{data/}
        C{subject04/}
            C{session01/}
                C{series09/}
                    C{120210_ex_B17_TT49.dcm} -> C{BreastChemo4/Visit1/dce_concat/120210 ex B17_TT49}
                    ...
"""

from nipype.interfaces.base import (traits,
    BaseInterfaceInputSpec, TraitedSpec, BaseInterface,
    InputMultiPath, OutputMultiPath, Directory)
from qipipe.staging.group_dicom import group_dicom_files


class GroupDicomInputSpec(BaseInterfaceInputSpec):
    subject_dirs = InputMultiPath(Directory(exists=True), mandatory=True,
        desc='The input subject directories to group')
    
    dest = Directory(exists=False, mandatory=True,
        desc='The output directory')
    
    dicom_pat = traits.Str(desc='The DICOM file glob pattern')
    
    session_pat = traits.Str(desc='The session subdirectory glob pattern')


class GroupDicomOutputSpec(TraitedSpec):
    series_dirs = OutputMultiPath(Directory(exists=True),
        desc='The output series directories')


class GroupDicom(BaseInterface):
    input_spec = GroupDicomInputSpec
    
    output_spec = GroupDicomOutputSpec

    def _run_interface(self, runtime):
        opts = dict(dest=self.inputs.dest)
        if self.inputs.dicom_pat:
            opts['dicom_pat'] = self.inputs.dicom_pat
        if self.inputs.session_pat:
            opts['session_pat'] = self.inputs.session_pat
        self.series_dirs = group_dicom_files(*self.inputs.subject_dirs, **opts)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['series_dirs'] = self.series_dirs
        return outputs
     