import os
from nipype.interfaces.base import (
    traits, CommandLine, CommandLineInputSpec, TraitedSpec, InputMultiPath,
    File)
from nipype.interfaces.traits_extension import isdefined


class XNATCopyInputSpec(CommandLineInputSpec):
    """
    The input spec with arguments in the following order:
    * options
    * the input files, for an upload
    * the XNAT object path
    * the destination directory, for a download
    """
    
    project = traits.Str(mandatory=True, desc='The XNAT project id')
    
    subject = traits.Str(mandatory=True, desc='The XNAT subject name')
    
    # The XNAT path masquerades as the session.
    # The XNAT path argument follows the input files for an upload and precedes
    # the destination for a download.
    session = traits.Str(mandatory=True, argstr='%s', position=-2,
                         desc='The XNAT session name')
    
    reconstruction = traits.Str(desc='The XNAT reconstruction name')
    
    assessor = traits.Str(desc='The XNAT assessor name')
    
    resource = traits.Str(desc='The XNAT resource name (scan default is NIFTI)')
    
    inout = traits.Str(desc='The XNAT reconstruction or assessor resource'
                            ' in/out qualifier')
    
    scan = traits.Either(traits.Int, traits.Str, desc='The XNAT scan name')
    
    # The input files to upload precede the XNAT path.
    # The input file arguments follow the options and precede the target
    # XNAT path.
    in_files = InputMultiPath(File(exists=True), argstr='%s', position=-3,
                              desc='The files to upload')
    
    modality = traits.Str(argstr='--modality %s',
                          desc="The XNAT scan modality, e.g. 'MR'")
    
    force = traits.Bool(argstr='--force',
                        desc='Flag indicating whether to replace an existing'
                             ' XNAT file')
    
    skip_existing = traits.Bool(argstr='--skip-existing',
                                desc='Flag indicating whether to skip upload'
                                     ' to an existing target XNAT file')
    
    # The download destination follows the XNAT path.
    # The destination goes at the end of the command line.
    dest = traits.Str(argstr='%s', position=-1, desc='The download directory')


class XNATCopyOutputSpec(TraitedSpec):
    xnat_files = traits.List(traits.Str, desc='The XNAT file object labels')


class XNATCopy(CommandLine):
    """
    The ``XNATCopy`` Nipype interface wraps the ``cpxnat`` command.
    """
    
    input_spec = XNATCopyInputSpec
    
    output_spec = XNATCopyOutputSpec
    
    cmd = 'cpxnat'
    
    def _format_path(self):
        """:return: the XNAT path prefixed by ``xnat:``"""
        
        # The XNAT object hierarchy starts with the subject and session.
        path = [self.inputs.project, self.inputs.subject, self.inputs.session]
        
        # The resource parent container.
        if isdefined(self.inputs.scan):
            path.append('scan')
            path.append(str(self.inputs.scan))
        elif isdefined(self.inputs.reconstruction):
            path.append('reconstruction')
            path.append(self.inputs.reconstruction)
        elif isdefined(self.inputs.assessor):
            path.append('assessor')
            path.append(self.inputs.assessor)
        
        # The resource.
        if isdefined(self.inputs.resource):
            if isdefined(self.inputs.inout):
                rsc_type = "%s_resource" % self.inputs.inout
            else:
                rsc_type = 'resource'
            path.append(rsc_type)
            path.append(self.inputs.resource)
        
        # Make the path string prefixed by xnat:.
        return 'xnat:/' + '/'.join(path)
    
    def _format_arg(self, opt, spec, val):
        if opt == 'session':
            return self._format_path()
        else:
            return super(XNATCopy, self)._format_arg(opt, spec, val)
    
    
    def _list_outputs(self):
        outputs = self._outputs().get()
        if isdefined(self.inputs.in_files):
            # The upload outputs the XNAT file names.
            outputs['xnat_files'] = [os.path.split(f)[1]
                                     for f in self.inputs.in_files]
        
        return outputs
