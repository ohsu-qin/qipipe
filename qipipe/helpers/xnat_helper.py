import os
import pyxnat

_XNAT_DOT_CFG = os.path.join(os.path.expanduser('~'), '.xnat', 'xnat.cfg')
"""The XNAT home C{.xnat} subdirectory configuration location."""

_XNAT_HOME_CFG = os.path.join(os.path.expanduser('~'), 'xnat.cfg')
"""The XNAT home configuration location."""

_XNAT_ETC_CFG = os.path.join('/etc', 'xnat.cfg')
"""The Linux global C{/etc} XNAT configuration location."""

class XNATError(Exception):
    pass

class XNAT(object):
    """XNAT is a pyxnat facade class for uploading files."""
    
    @classmethod
    def default_configuration(self):
        """
        Returns the XNAT configuration file location determined as the first file found
        in the following precedence order:
            1. C{xnat.cfg} in the home C{.xnat} subdirectory
            2. C{xnat.cfg} in the home directory
            3. C{xnat.cfg} in the C{/etc} directory

        @return: the configuration location, if any
        """
        for f in [_XNAT_DOT_CFG, _XNAT_HOME_CFG, _XNAT_ETC_CFG]:
            if os.path.exists(f):
                return f
    
    def __init__(self, config_or_interface=None):
        """
        @param config_or_interface: the configuration file, pyxnat Interface,
            or None to connect with the L{default_configuration}
        """
        if isinstance(config_or_interface, pyxnat.Interface):
            self.xf = config_or_interface
        else:
            if not config_or_interface:
                config = XNAT.default_configuration()   
            self.xf = pyxnat.Interface(config=config)
    
    def upload(self, project, subject, session, path, **kwargs):
        """
        Imports the given file into the XNAT resource with the following hierarchy:
    
            /project/PROJECT/subject/SUBJECT/experiment/SESSION/I{container}/CONTAINER/resource/FORMAT
    
        where:
            -  the XNAT experiment label is the C{session} parameter
            -  I{container} is the experiment child type, e.g. C{scan}
            -  the XNAT resource label is the file format, e.g. C{NIFTI} or C{DICOM}
        
        The keyword arguments include the session child container, scan C{modality} and file C{format}.
        The required container keyword argument associates the container type to the container label,
        e.g. C{scan=1}. The container type is C{scan}, C{reconstruction} or C{analysis}.
        The C{analysis} container type value corresponds to the XNAT C{assessor} Image Assessment type.
        C{analysis}, C{assessment} and C{assessor} are synonymous.
        The container label can be a string or integer, e.g. the series number.
        
        If the file extension is C{.nii}, C{.nii.gz}, C{dcm} opr C{.dcm.gz}, then the format
        is inferred from the extension. Otherwise, the format keyword argument is required.
        
        If the session does not yet exist as a XNAT experiment, then the modality keyword argument
        is required. The modality is any supported XNAT modality, e.g. C{MR} or  or C{CT}. A capitalized
        modality value is a synonym for the XNAT session data type, e.g. C{MR} is a synonym for
        C{xnat:mrSessionData}.
        
        Example:
    
           upload('TCIA', 'Patient04', 'PT04_MR1', 'data/pt4/visit4/image003.nii.gz', scan=4, modality='MR')
           XNATRestClient <options> -m GET -remote \
              "/data/archive/projects/TCIA/subjects/Patient04/experiments/PT04_MR1/scans/4/resources/NIFTI/files/image003.nii.gz" \
              >/tmp/image003.nii.gz

        @param project: the XNAT project id
        @param subject: the XNAT subject label
        @param session: the session (XNAT experiment) label
        @param path: the path of the file to upload
        @param kwargs: the session child container, file format and scan modality
        @raise XNATError: if the project does not exist
        """
    
        # The XNAT project, which must already exist.
        prj = self.xf.select.project(project)
        if not prj.exists():
            raise XNATError("XNAT upload project not found: %s" % project)
        # The keyword arguments.
        modality = kwargs.pop('modality', None)
        format = kwargs.pop('format', None)
        if not kwargs:
            raise XNATError("XNAT upload is missing the session child container")
        container_type, container_label = kwargs.items()[0]
        # The XNAT experiment.
        exp = prj.subject(subject).experiment(session)
        # If the experiment must be created, then the experiment create parameters
        # consists of the session modality data type.
        if exp.exists():
            params = {}
        elif modality:
            if not modality.startswith('xnat:'):
                if not modality.endswith('SessionData'):
                    if modality.isupper():
                        modality = modality.lower()
                    modality = modality + 'SessionData'
                modality = 'xnat:' + modality
            params = dict(experiments=modality)
        else:
            raise XNATError("XNAT upload is missing the modality for session: %s" % session)
    
        # The file format.
        _, fname = os.path.split(path)
        if not format:
            root, ext = os.path.splitext(fname)
            if ext == '.gz':
                _, ext = os.path.splitext(root)
            if ext == '.nii':
                format = 'NIFTI'
            elif ext == '.dcm':
                format = 'DICOM'
            else:
                raise XNATError("XNAT upload is missing the format for the file: %s" % path)
    
        # The resource parent container.
        rsc_parent_label = str(container_label)
        if container_type == 'scan':
            rsc_parent = exp.scan(rsc_parent_label)
        elif container_type == 'reconstruction':
            rsc_parent = exp.reconstruction(rsc_parent_label)
        elif container_type in ('analysis', 'assessment', 'assessor'):
            rsc_parent = exp.assessor(rsc_parent_label)
        else:
            raise XNATError("XNAT upload session child container not recognized: " + container_type)
        # Upload the file.
        rsc_parent.resource(format).file(fname).insert(path)
