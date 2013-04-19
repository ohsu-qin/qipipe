import os, re
import pyxnat
from pyxnat.core.resources import Reconstruction, Reconstructions
from pyxnat.core.errors import DatabaseError
from .xnat_config import default_configuration

import logging
logger = logging.getLogger(__name__)

class XNATError(Exception):
    pass

def facade():
    """
    @return the XNAT facade, created on demand
    """
    if not hasattr(facade, 'instance'):
        facade.instance = XNAT()
    return facade.instance

class XNAT(object):
    """XNAT is a pyxnat facade convenience class."""
    
    SESSION_QUERY_FMT = "/project/%s/subject/%s/experiment/%s"
    """The session query template."""
    
    CONTAINER_TYPES = ['scan', 'reconstruction', 'assessor']
    """The supported XNAT resource container types."""
    
    ASSESSOR_SYNONYMS = ['analysis', 'assessment']
    """Alternative designations for the XNAT C{assessor} container type."""
    
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
    
    def get_session(self, project, subject, session):
        """
        @param project: the XNAT project id
        @param subject: the XNAT subject label
        @param session: the session (XNAT experiment) label
        @return: the corresponding XNAT session (which may not exist)
        """
        # The query path.
        qpath = XNAT.SESSION_QUERY_FMT % (project, subject, session)
        return self.interface.select(qpath)
    
    def session_exists(self, project, subject, session):
        """
        @param project: the XNAT project id
        @param subject: the XNAT subject label
        @param session: the session (XNAT experiment) label
        @return: whether the session exists in XNAT
        """
        return self.get_session (project, subject, session).exists()
    
    def download(self, project, subject, session, **opts):
        """
        Downloads the files for the specfied XNAT session.
        
        The keyword options include the format and session child container.
        The session child container option can be set to a specific resource container,
        e.g. C{scan=1}, as described in L{XNAT.upload}, or all resources of a given
        container type. In the latter case, the C{container_type} parameter is set.
        The permissible container types are described in L{XNAT.upload}.
        
        @param project: the XNAT project id
        @param session: the XNAT experiment label
        @param opts: the resource selection options
        @keyword format: the image file format (C{NIFTI} or C{DICOM})
        @keyword scan: the scan number
        @keyword reconstruction: the reconstruction label
        @keyword analysis: the analysis label
        @keyword container_type: the container type, if no specific container is specified
        @keyword inout: @param inout: the C{in}/C{out} reconstruction resource qualifier
        @keyword dest: the optional download location (default current directory)
        """
        
        # The XNAT experiment, which must exist.
        query = XNAT.SESSION_QUERY_FMT % (project, subject, session)
        exp = self.interface.select(query)
        if not exp.exists():
            raise XNATError("XNAT download session not found: %s" % session)
        
        # The download location.
        dest = opts.pop('dest', None) or os.getcwd()
        if not os.path.exists(dest):
            os.makedirs(dest)
        
        logger.debug("Downloading the %s files to %s..." % (session, dest))

        # The resource.
        rsc = self._infer_xnat_resource(exp, opts)
        format = opts['format']
        
        # Download the files.
        return [self._download_file(f, dest) for f in rsc.files()]
        
    def _download_file(self, file_obj, dest):
        """
        @param file_obj: the XNAT file object
        @param dest: the target directory
        @return: the downloaded file path
        """
        
        fname = file_obj.label()
        tgt = os.path.join(dest, fname)
        file_obj.get(tgt)
        logger.debug("Downloaded the XNAT file %s." % tgt)
        
        return tgt
    
    def upload(self, project, subject, session, *in_files, **opts):
        """
        Imports the given files into the XNAT resource with the following hierarchy:
    
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
    
        >>> from qipipe.helpers import xnat_helper
        >>> xnat = xnat_helper.facade()
        >>> xnat.upload('QIN', 'Sarcoma003', 'Sarcoma003_Session01', scan=4, modality='MR',
        >>>    format='NIFTI', *in_files)

        @param project: the XNAT project id
        @param subject: the XNAT subject label
        @param session: the session (XNAT experiment) label
        @param in_files: the input files to upload
        @param opts: the session child container, file format, scan modality and optional additional
            XNAT file creation options
        @keyword scan: the scan number
        @keyword reconstruction: the reconstruction label
        @keyword analysis: the analysis label
        @keyword modality: the session modality
        @keyword format: the image format
        @return: the new XNAT file names
        @raise XNATError: if the project does not exist
        @raise XNATError: if the session child resource container type option is missing
        @raise XNATError: if the XNAT experiment does not exist and the modality option is missing
        """
    
        # The XNAT project, which must already exist.
        prj = self.interface.select.project(project)
        if not prj.exists():
            raise XNATError("XNAT upload project not found: %s" % project)
        
        # The XNAT experiment.
        exp = prj.subject(subject).experiment(session)

        # If the experiment must be created, then the experiment create parameters
        # consists of the session modality data type.
        modality = opts.pop('modality', None)
        if not exp.exists:
            if not modality:
                raise XNATError("XNAT upload is missing the modality for session: %s" % session)
            if not modality.startswith('xnat:'):
                if not modality.endswith('SessionData'):
                    if modality.isupper():
                        modality = modality.lower()
                    modality = modality + 'SessionData'
                modality = 'xnat:' + modality
            opts['experiments'] = modality
            exp.create()
        
        # The resource which contains the files.
        rsc = self._infer_xnat_resource(exp, opts)
        
        # Upload each file.
        logger.debug("Uploading the %s files to XNAT..." % session)
        xnat_files = [self._upload_file(rsc, f, **opts) for f in in_files]
        logger.debug("%s files uploaded to XNAT." % session)
        
        return xnat_files
    
    def _infer_xnat_resource(self, experiment, opts):
        """
        Infers the resource container item from the given options.
        
        @param experiment: the XNAT experiment object
        @param opts: the L{XNAT.download} options
        @return: the container (type, value) tuple
        """
        
        # The image format.
        if not opts.has_key('format'):
            raise XNATError("XNAT upload is missing the image format for session: %s" % experiment.label())
        format = opts['format']

        # The resource parent type and name.
        ctr_type, ctr_label = self._infer_resource_container(opts)
        # The resource parent.
        rsc_parent = self._xnat_resource_parent(experiment, ctr_type, ctr_label)
        
        # The resource.
        rsc = self._xnat_child_resource(rsc_parent, opts['format'], opts.get('inout'))
        logger.debug("The XNAT ")
        return rsc
    
    def _infer_resource_container(self, opts):
        """
        Finds and removes the resource container item from the given options.
        
        @param opts: the options to check
        @return: the container (type, value) tuple, or (None, None) if not found
        """
        
        if opts.has_key('container_type'):
            return (opts['container_type'], None)
        for t in XNAT.CONTAINER_TYPES:
            if opts.has_key(t):
                return (t, opts[t])
        for t in XNAT.ASSESSOR_SYNONYMS:
            if opts.has_key(t):
                return ('assessor', opts[t])
        raise XNATError("Resource container could not be inferred from options %s" % opts)
    
    def _xnat_container_type(self, name):
        """
        @param name: the L{XNAT.CONTAINER_TYPES} or L{XNAT.ASSESSOR_SYNONYMS} container designator
        @return: the standard XNAT designator in L{XNAT.CONTAINER_TYPES}
        @raise XNATError: if the name does not designate a valid container type
        """
        
        if name in XNAT.ASSESSOR_SYNONYMS:
            return 'assessor'
        elif name in XNAT.CONTAINER_TYPES:
            return name
        else:
            raise XNATError("XNAT upload session child container not recognized: " + name)
    
    def _xnat_resource_parent(self, experiment, container_type, name=None):
        """
        @param experiment: the XNAT experiment
        @param container_type: the container type in L{XNAT.CONTAINER_TYPES}
        @param name: the optional container name, e.g. C{NIFTI}
        @return: the XNAT resource parent object
        """
        
        if name:
            name = str(name)
            if container_type == 'scan':
                return experiment.scan(name)
            elif container_type == 'reconstruction':
                return experiment.reconstruction(name)
            elif container_type == 'assessor':
                return experiment.assessor(name)
        elif container_type == 'scan':
            return experiment.scans()
        elif container_type == 'reconstruction':
            return experiment.reconstructions()
        elif container_type == 'assessor':
            return experiment.assessors()
        raise XNATError("XNAT resource container type not recognized: %s" % container_type)
    
    def _xnat_child_resource(self, parent, name=None, inout=None):
        """
        @param parent: the XNAT resource parent object
        @param name: the resource name, e.g. C{NIFTI}
        @param inout: the reconstruction in/out option described in L{XNAT.download}
        @return: the XNAT resource object
        @raise XNATError: if the inout option is invalid
        """
        
        if name:
            if isinstance(parent, Reconstruction) or isinstance(parent, Reconstructions):
                if inout == 'in':
                    return parent.in_resource(name)
                elif inout in ['out', None]:
                    return parent.out_resource(name)
                else:
                    raise XNATError("Unsupported resource inout option: %s" % inout)
            else:
                return parent.resource(name)
        elif isinstance(parent, Reconstruction) or isinstance(parent, Reconstructions):
            if inout == 'in':
                return parent.in_resources()
            elif inout in ['out', None]:
                return parent.out_resources()
            else:
                raise XNATError("Unsupported resource inout option: %s" % inout)
        else:
            return parent.resources()
        
    def _infer_format(self, in_file):
        """
        Infers the given image file format from the file extension
        @param in_file: the input file path
        @return: the image format, or None if the format could not be inferred
        """
        
        # The XNAT file name.
        _, fname = os.path.split(in_file)

        # Infer the format, if necessary.
        base, ext = os.path.splitext(fname)
        # Ignore .gz to get at the format extension.
        if ext == '.gz':
            _, ext = os.path.splitext(base)
        if ext == '.nii':
            return 'NIFTI'
        elif ext == '.dcm':
            return 'DICOM'
        
    def _upload_file(self, resource, in_file, **opts):
        """
        Uploads the given file to XNAT.
        
        @param resource: the XNAT resource that contains the file
        @param in_file: the input file path
        @param opts: the XNAT file options
        @return: the XNAT file name
        """
        
        # The XNAT file name.
        _, fname = os.path.split(in_file)
        
        # Infer the format, if necessary.
        if not opts.has_key('format'):
            format = self._infer_format(in_file)
            if format:
                opts['format'] = format
        
        # Make the resource, if necessary.
        if not resource.exists():
            try:
                resource.create()
            except DatabaseError as e:
                logger.error("Error uploading the XNAT file %s: %s" % (fname, e))
                logger.error("The XNAT resource might be archived, although the resource object doesn't exist.")
                logger.error("If the archive directory exists, then delete it manually and retry.")
                raise
        
        # The XNAT file wrapper.
        file_obj = resource.file(fname)
        # Check for an existing file.
        if file_obj.exists():
            raise XNATError("%s file already exists: %s" % (resource.parent().label(), fname))
        
        # Upload the file.
        logger.debug("Uploading the XNAT file %s from %s..." % (fname, in_file))
        file_obj.insert(in_file, **opts)
        logger.debug("Uploaded the XNAT file %s." % fname)
    
        return fname
