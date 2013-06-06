import os, re
from contextlib import contextmanager, closing
import pyxnat
from pyxnat.core.resources import Reconstruction, Reconstructions
from pyxnat.core.errors import DatabaseError
from .. import PROJECT
from .xnat_config import default_configuration

import logging
logger = logging.getLogger(__name__)

@contextmanager
def connection():
    """
    Returns the sole L{XNAT} connection. The connection is closed
    when the outermost connection block finishes.
    
    Example:
        >>> from qipipe.helpers import xnat_helper
        >>> with xnat_helper.connection() as xnat:
        >>>    sbj = xnat.get_subject(PROJECT, 'Breast003')

    :return a L{XNAT} instance
    """
    if hasattr(connection, 'xnat'):
        yield connection.xnat
    else:
        with closing(XNAT()) as xnat:
            connection.xnat = xnat
            yield xnat
            del connection.xnat

def delete_subjects(project, *subject_names):
    """
    Deletes the given XNAT subjects, if they exist.
    
    :param project: the XNAT project id
    :param subject_names: the XNAT subject names
    """
    with connection() as xnat:
        for sbj_lbl in subject_names:
            sbj = xnat.get_subject(project, sbj_lbl)
            if sbj.exists():
                sbj.delete()
                logger.debug("Deleted the XNAT test subject %s." % sbj_lbl)


class XNATError(Exception):
    pass


class XNAT(object):
    """XNAT is a pyxnat facade convenience class."""
    
    SUBJECT_QUERY_FMT = "/project/%s/subject/%s"
    """The subject query template."""
    
    CONTAINER_TYPES = ['scan', 'reconstruction', 'assessor']
    """The supported XNAT resource container types."""
    
    ASSESSOR_SYNONYMS = ['analysis', 'assessment']
    """Alternative designations for the XNAT C{assessor} container type."""
    
    def __init__(self, config_or_interface=None):
        """
        :param config_or_interface: the configuration file, pyxnat Interface,
            or None to connect with the L{default_configuration}
        """
        if isinstance(config_or_interface, pyxnat.Interface):
            self.interface = config_or_interface
        else:
            if not config_or_interface:
                config_or_interface = default_configuration()
            logger.debug("Connecting to XNAT with config %s..." % config_or_interface)
            self.interface = pyxnat.Interface(config=config_or_interface)
    
    def close(self):
        """Drops the XNAT connection."""
        self.interface.disconnect()
        logger.debug("Closed the XNAT connection.")
    
    def get_subject(self, project, subject):
        """
        Returns the XNAT subject object for the given XNAT lineage.
        
        :param project: the XNAT project id
        :param subject: the XNAT subject label
        :return: the corresponding XNAT subject (which may not exist)
        """
        # The query path.
        qpath = XNAT.SUBJECT_QUERY_FMT % (project, subject)
        return self.interface.select(qpath)
    
    def get_session(self, project, subject, session):
        """
        Returns the XNAT session object for the given XNAT lineage.
        The session name is qualified by the subject name prefix, if necessary.
        
        :param project: the XNAT project id
        :param subject: the XNAT subject label
        :param session: the XNAT experiment label
        :return: the corresponding XNAT session (which may not exist)
        """
        sess_lbl = self._canonical_session_label(subject, session)
        
        return self.get_subject(project, subject).experiment(sess_lbl)
    
    def get_scan(self, project, subject, session, scan):
        """
        Returns the XNAT scan object for the given XNAT lineage.
        
        @see: L{get_session}
        :param project: the XNAT project id
        :param subject: the XNAT subject label
        :param session: the XNAT experiment label
        :param scan: the XNAT scan name or number
        :return: the corresponding XNAT scan object (which may not exist)
        """
        return self.get_session(project, subject, session).scan(str(scan))
      
    def get_reconstruction(self, project, subject, session, recon):
        """
        Returns the XNAT reconstruction object for the given XNAT lineage.
        The session and reconstruction name is qualified by the session name prefix,
        if necessary.
        
        @see: L{get_session}
        :param project: the XNAT project id
        :param subject: the XNAT subject label
        :param session: the XNAT experiment label
        :param recon: the unique XNAT reconstruction name
        :return: the corresponding XNAT reconstruction object (which may not exist)
        """
        sess_lbl = self._canonical_session_label(subject, session)
        if recon.startswith(sess_lbl):
            recon_lbl = recon
        else:
            recon_lbl = "%s_%s" % (sess_lbl, recon)
        
        return self.get_session(project, subject, session).reconstruction(recon_lbl)
    
    def _canonical_session_label(self, subject, session):
        """
        Returns the XNAT session name, qualified by the subject name prefix if necessary.
        
        :param subject: the XNAT subject label
        :param session: the XNAT experiment label
        :return: the corresponding XNAT session label
        """
        if session.startswith(subject):
            return session
        else:
            return "%s_%s" % (subject, session)
    
    def download(self, project, subject, session, **opts):
        """
        Downloads the files for the specfied XNAT session.
        
        The keyword options include the format and session child container.
        The session child container option can be set to a specific resource container,
        e.g. C{scan=1}, as described in L{XNAT.upload}, or all resources of a given
        container type. In the latter case, the C{container_type} parameter is set.
        The permissible container types are described in L{XNAT.upload}.
        
        A reconstruction option value is qualified by the session label, if necessary.
        
        :param project: the XNAT project id
        :param subject: the XNAT subject label
        :param session: the XNAT experiment label
        :param opts: the resource selection options
        :keyword format: the image file format (C{NIFTI} or C{DICOM})
        :keyword scan: the scan number
        :keyword reconstruction: the reconstruction name
        :keyword analysis: the analysis name
        :keyword container_type: the container type, if no specific container is specified
        :keyword inout: :param inout: the C{in}/C{out} reconstruction resource qualifier
        :keyword dest: the optional download location (default current directory)
        """
        # The XNAT experiment, which must exist.
        exp = self.get_session(project, subject, session)
        if not exp.exists():
            raise XNATError("The XNAT download session was not found: %s" % session)
        
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
        :param file_obj: the XNAT file object
        :param dest: the target directory
        :return: the downloaded file path
        """
        fname = file_obj.label()
        if not fname:
            raise XNATError("XNAT file object does not have a name: %s" % file_obj)
        tgt = os.path.join(dest, fname)
        file_obj.get(tgt)
        logger.debug("Downloaded the XNAT file %s." % tgt)
        
        return tgt
    
    def upload(self, project, subject, session, *in_files, **opts):
        """
        Imports the given files into the XNAT resource with the following hierarchy:
    
            /project/PROJECT/subject/SUBJECT/experiment/SESSION/I{container}/CONTAINER/resource/FORMAT
    
        where:
        -  the XNAT experiment name is the C{session} parameter
        -  I{container} is the experiment child type, e.g. C{scan}
        -  the XNAT resource name is the file format, e.g. C{NIFTI} or C{DICOM}
        
        The keyword options include the session child container, scan C{modality} and file C{format}.
        The required container keyword argument associates the container type to the container name,
        e.g. C{scan=1}. The container type is C{scan}, C{reconstruction} or C{analysis}.
        The C{analysis} container type value corresponds to the XNAT C{assessor} Image Assessment type.
        C{analysis}, C{assessment} and C{assessor} are synonymous.
        The container name can be a string or integer, e.g. the series number.
        
        If the XNAT file extension is C{.nii} or C{dcm}, then the default XNAT image format
        is inferred from the extension. Otherwise, the C{format} keyword option is required.
        
        If the session does not yet exist as a XNAT experiment, then the modality keyword argument
        is required. The modality is any supported XNAT modality, e.g. C{MR} or  or C{CT}. A capitalized
        modality value is a synonym for the XNAT session data type, e.g. C{MR} is a synonym for
        C{xnat:mrSessionData}.
        
        Example:
    
        >>> from qipipe.helpers import xnat_helper
        >>> xnat = xnat_helper.facade()
        >>> xnat.upload(PROJECT, 'Sarcoma003', 'Sarcoma003_Session01', scan=4, modality='MR',
        >>>    format='NIFTI', *in_files)

        :param project: the XNAT project id
        :param subject: the XNAT subject name
        :param session: the session (XNAT experiment) name
        :param in_files: the input files to upload
        :param opts: the session child container, file format, scan modality and optional additional
            XNAT file creation options
        :keyword scan: the scan number
        :keyword reconstruction: the reconstruction name
        :keyword analysis: the analysis name
        :keyword modality: the session modality
        :keyword format: the image format
        :return: the new XNAT file names
        :raise XNATError: if the project does not exist
        :raise XNATError: if the session child resource container type option is missing
        :raise XNATError: if the XNAT experiment does not exist and the modality option is missing
        """
        # The XNAT experiment.
        exp = self.get_session(project, subject, session)

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
            logger.debug("Creating the XNAT %s experiment..." % session)
            exp.create()
            logger.debug("Created the XNAT experiment %s." % session)

        # Make the resource parent container, if necessary.
        ctr_type, ctr_id = self._infer_resource_container(opts)
        ctr = self._xnat_resource_parent(exp, ctr_type, ctr_id)
        if not ctr.exists():
            logger.debug("Creating the XNAT %s resource parent container %s..." % (session, ctr_id))
            ctr.create()
            logger.debug("Created the XNAT %s resource parent container with id %s." % (session, ctr.id()))

        format = opts['format']
        # Infer the format, if necessary.
        if not format:
            format = self._infer_format(*in_files)
            if format:
                opts['format'] = format
            else:
                raise XNATError("XNAT %s upload cannot infer the image format" % session)

        # The XNAT resource that will hold the files.
        rsc = self._xnat_child_resource(ctr, format, opts.pop('inout', None))
        # Make the resource, if necessary.
        if not rsc.exists():
            logger.debug("Creating the XNAT %s %s %s resource..." % (session, ctr_id, format))
            rsc.create()
            logger.debug("Created the XNAT %s %s resource with name %s and id %s." % (session, ctr_id, rsc.label(), rsc.id()))
        
        # Upload each file.
        logger.debug("Uploading the %s files to XNAT..." % session)
        xnat_files = [self._upload_file(rsc, f, opts) for f in in_files]
        logger.debug("%s files uploaded to XNAT." % session)
        
        return xnat_files
    
    def _infer_xnat_resource(self, experiment, opts):
        """
        Infers the resource container item from the given options.
        
        :param experiment: the XNAT experiment object
        :param opts: the L{XNAT.download} options
        :return: the container (type, value) tuple
        """
        # The image format.
        if not opts.has_key('format'):
            raise XNATError("XNAT upload is missing the image format for session: %s" % experiment.label())
        format = opts['format']

        # The resource parent type and name.
        ctr_type, ctr_name = self._infer_resource_container(opts)
        # The resource parent.
        rsc_parent = self._xnat_resource_parent(experiment, ctr_type, ctr_name)
        
        # The resource.
        return self._xnat_child_resource(rsc_parent, opts['format'], opts.get('inout'))
    
    def _infer_resource_container(self, opts):
        """
        Finds and removes the resource container item from the given options.
        
        :param opts: the options to check
        :return: the container (type, value) tuple, or (None, None) if not found
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
        :param name: the L{XNAT.CONTAINER_TYPES} or L{XNAT.ASSESSOR_SYNONYMS} container designator
        :return: the standard XNAT designator in L{XNAT.CONTAINER_TYPES}
        :raise XNATError: if the name does not designate a valid container type
        """
        if name in XNAT.ASSESSOR_SYNONYMS:
            return 'assessor'
        elif name in XNAT.CONTAINER_TYPES:
            return name
        else:
            raise XNATError("XNAT upload session child container not recognized: " + name)
    
    def _xnat_resource_parent(self, experiment, container_type, name=None):
        """
        Return the resource parent for the given experiment and container type.
        The resource parent is the experiment child with the given container type,
        e.g a MR session scan or reconstruction. If there is a name, then the parent
        is the object with that name, e.g. C{reconstruction('reg_1')}. Otherwise,
        the parent is a container group, e.g. C{reconstructions}.
        
        :param experiment: the XNAT experiment
        :param container_type: the container type in L{XNAT.CONTAINER_TYPES}
        :param name: the optional container name, e.g. C{NIFTI}
        :return: the XNAT resource parent object
        """
        if name:
            # Convert an integer name, e.g. scan number, to a string.
            name = str(name)
            # The parent is the session child for the given container type.
            if container_type == 'scan':
                return experiment.scan(name)
            elif container_type == 'reconstruction':
                # The recon id is qualified by the experiment label.
                prefix = experiment.label()
                if name.startswith(prefix):
                   recon_id = name
                else:
                    recon_id = "%s_%s" % (prefix, name)
                return experiment.reconstruction(recon_id)
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
        :param parent: the XNAT resource parent object
        :param name: the resource name, e.g. C{NIFTI}
        :param inout: the reconstruction in/out option described in L{XNAT.download}
        :return: the XNAT resource object
        :raise XNATError: if the inout option is invalid
        """
        if name:
            if isinstance(parent, Reconstruction) or isinstance(parent, Reconstructions):
                if inout == 'in':
                    rsc = parent.in_resource(name)
                elif inout in ['out', None]:
                    rsc = parent.out_resource(name)
                else:
                    raise XNATError("Unsupported resource inout option: %s" % inout)
                return rsc
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
        
    def _infer_format(self, *in_files):
        """
        Infers the given image file format from the file extension
        :param in_files: the input file paths
        :return: the image format, or None if the format could not be inferred
        :raise XNATError: if the input files don't have the same file extension
        """
        # A sample input file.
        in_file = in_files[0]
        # The XNAT file name.
        _, fname = os.path.split(in_file)
        # Infer the format from the extension.
        base, ext = os.path.splitext(fname)

        # Verify that all remaining input files have the same extension.
        if in_files:
            for f in in_files[1:]:
                _, other_ext = os.path.splitext(f)
                if ext != other_ext:
                    raise XNATError("Upload cannot determine a format from heterogeneous file extensions: %s vs %f" % (fname, f))

        # Ignore .gz to get at the format extension.
        if ext == '.gz':
            _, ext = os.path.splitext(base)
        if ext == '.nii':
            return 'NIFTI'
        elif ext == '.dcm':
            return 'DICOM'
        
    def _upload_file(self, resource, in_file, opts):
        """
        Uploads the given file to XNAT.
        
        :param resource: the existing XNAT resource that contains the file
        :param in_file: the input file path
        :param opts: the XNAT file options
        :return: the XNAT file name
        :raise XNATError: if the XNAT file already exists
        """
        # The XNAT file name.
        _, fname = os.path.split(in_file)
        logger.debug("Uploading the XNAT file %s from %s..." % (fname, in_file))

        # The XNAT file wrapper.
        file_obj = resource.file(fname)
        # The resource parent container.
        rsc_ctr = resource.parent()
        # Check for an existing file.
        if file_obj.exists():
            raise XNATError("The XNAT file object %s already exists in the %s %s resource" %
                (fname, rsc_ctr.id(), resource.label()))
        
        # Upload the file.
        logger.debug("Inserting the XNAT file %s into the %s %s %s resource..." %
            (fname, rsc_ctr.__class__.__name__.lower(), rsc_ctr.id(), resource.label()))
        file_obj.insert(in_file, **opts)
        logger.debug("Uploaded the XNAT file %s." % fname)
    
        return fname
