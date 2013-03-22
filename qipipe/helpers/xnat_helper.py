import os, re
import pyxnat
from .xnat_config import default_configuration

import logging
logger = logging.getLogger(__name__)

class XNATError(Exception):
    pass

class XNAT(object):
    """XNAT is a pyxnat facade convenience class."""
    
    SESSION_QUERY_FMT = "/project/%s/subject/%s/experiment/%s"
    """The session query template."""
    
    def __init__(self, config_or_interface=None):
        """
        @param config_or_interface: the configuration file, pyxnat Interface,
            or None to connect with the L{default_configuration}
        """
        if isinstance(config_or_interface, pyxnat.Interface):
            self.interface = config_or_interface
        else:
            if not config_or_interface:
                config = default_configuration()   
            self.interface = pyxnat.Interface(config=config)
    
    def session_exists(self, project, subject, session):
        """
        @param project: the XNAT project id
        @param subject: the XNAT subject label
        @param session: the session (XNAT experiment) label
        @return: whether the session exists in XNAT
        """
        # Make the query path
        qpath = XNAT.SESSION_QUERY_FMT % (project, subject, session)
        return self.interface.select(qpath).exists()
    
    def upload(self, project, subject, session, *in_files, **opts):
        """
        Imports the given file into the XNAT resource with the following hierarchy:
    
            /project/PROJECT/subject/SUBJECT/experiment/SESSION/I{container}/CONTAINER/resource/FORMAT
    
        where:
            -  the XNAT experiment label is the C{session} parameter
            -  I{container} is the experiment child type, e.g. C{scan}
            -  the XNAT resource label is the file format, e.g. C{NIFTI} or C{DICOM}
        
        The keyword options include the session child container, scan C{modality} and file C{format}.
        The required container keyword argument associates the container type to the container label,
        e.g. C{scan=1}. The container type is C{scan}, C{reconstruction} or C{analysis}.
        The C{analysis} container type value corresponds to the XNAT C{assessor} Image Assessment type.
        C{analysis}, C{assessment} and C{assessor} are synonymous.
        The container label can be a string or integer, e.g. the series number.
        
        If the XNAT file extension is C{.nii} or C{dcm}, then the default XNAT image format
        is inferred from the extension. Otherwise, the C{format} keyword option is required.
        
        If the session does not yet exist as a XNAT experiment, then the modality keyword argument
        is required. The modality is any supported XNAT modality, e.g. C{MR} or  or C{CT}. A capitalized
        modality value is a synonym for the XNAT session data type, e.g. C{MR} is a synonym for
        C{xnat:mrSessionData}.
        
        Example:
    
           upload('TCIA', 'Sarcoma03', 'Sarcoma03_Session01', 'data/pt4/p4v3/image003.nii.gz', scan=4, modality='MR')
           XNATRestClient <options> -m GET -remote \
              "/data/archive/projects/QIN/subjects/Sarcoma03/experiments/Sarcoma03_Session01/scans/4/resources/NIFTI/files/image003.nii.gz" \
              >/tmp/mr_image003.nii.gz

        @param project: the XNAT project id
        @param subject: the XNAT subject label
        @param session: the session (XNAT experiment) label
        @param in_files: the input files to upload
        @param opts: the session child container, file format and scan modality options
        @return: the new XNAT file names
        @raise XNATError: if the project does not exist
        @raise XNATError: if the session child resource container type option is missing
        @raise XNATError: if the XNAT experiment does not exist and the modality option is missing
        """
    
        # The XNAT project, which must already exist.
        prj = self.interface.select.project(project)
        if not prj.exists():
            raise XNATError("XNAT upload project not found: %s" % project)
        # The keyword arguments.
        modality = opts.pop('modality', None)
        format = opts.pop('format', None)
        if not opts:
            raise XNATError("XNAT upload is missing the session child container")
        container_type, container_label = opts.items()[0]
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
        
        # Upload each file.
        logger.debug("Uploading the %s files to XNAT..." % session)
        xnat_files = [self._upload_file(f, rsc_parent, format, params) for f in in_files]
        logger.debug("%s files uploaded to XNAT." % session)
        
        return xnat_files
            
        
    def _upload_file(self, in_file, rsc_parent, format, params):
        """
        Uploads the given file to XNAT.
        @param in_file: the input file path
        @param rsc_parent: the XNAT resource parent container
        @param format: the file image format, or None to infer the format from the extension
        @param params: the optional additional XNAT insert parameters
        @return: the XNAT file name
        @raise XNATError: if the image format could not be inferred from the file extension
        """
        # The XNAT file name.
        _, fname = os.path.split(in_file)

        # Infer the format, if necessary.
        if not format:
            base, ext = os.path.splitext(fname)
            # Ignore .gz to get at the format extension.
            if ext == '.gz':
                _, ext = os.path.splitext(base)
            if ext == '.nii':
                format = 'NIFTI'
            elif ext == '.dcm':
                format = 'DICOM'
            else:
                raise XNATError("XNAT upload is missing the format for the file: %s" % path)

        # Upload the file.
        logger.debug("Uploading the XNAT file %s from %s..." % (fname, in_file))
        rsc_parent.resource(format).file(fname).insert(in_file, *params)
        logger.debug("Uploaded the XNAT file %s." % fname)
    
        return fname
