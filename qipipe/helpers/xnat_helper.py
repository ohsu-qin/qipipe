import os, re
from contextlib import contextmanager, closing
import pyxnat
from pyxnat.core.resources import Reconstruction, Reconstructions, Assessor, Assessors
from pyxnat.core.errors import DatabaseError
from .xnat_config import default_configuration

import logging
logger = logging.getLogger(__name__)

@contextmanager
def connection():
    """
    Returns the sole :class:`XNAT` connection. The connection is closed
    when the outermost connection block finishes.
    
    Example:
        >>> from qipipe.helpers import xnat_helper
        >>> with xnat_helper.connection() as xnat:
        ...    sbj = xnat.get_subject(project(), 'Breast003')

    :return a :class:`XNAT` instance
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
    
def canonical_label(*names):
    """
    Returns the XNAT label for the given hierarchical name, qualified by
    a prefix if necessary.
    
    Example:
    
    >>> from qipipe.helpers.xnat_helper import canonical_label
    >>> canonical_label('QIN', 'Breast003', 'Session01')
    'QIN_Breast003_Session01'
    >>> canonical_label('QIN', 'Breast003', 'QIN_Breast003_Session01')
    'QIN_Breast003_Session01'
    
    :param names: the object names
    :return: the corresponding XNAT label
    """
    names = list(names)
    last = names.pop()
    if names:
        prefix = canonical_label(*names)
        if last.startswith(prefix):
            return last
        else:
            return "%s_%s" % (prefix, last)
    else:
        return last


class XNATError(Exception):
    pass


class XNAT(object):
    """XNAT is a pyxnat facade convenience class."""
    
    SUBJECT_QUERY_FMT = "/project/%s/subject/%s"
    """The subject query template."""
    
    CONTAINER_TYPES = ['scan', 'reconstruction', 'assessor']
    """The supported XNAT resource container types."""
    
    ASSESSOR_SYNONYMS = ['analysis', 'assessment']
    """Alternative designations for the XNAT ``assessor`` container type."""
    
    INOUT_CONTAINER_TYPES = [Reconstruction, Reconstructions, Assessor, Assessors]
    
    def __init__(self, config_or_interface=None):
        """
        
        :param config_or_interface: the configuration file, pyxnat Interface,
            or None to connect with the :meth:`default_configuration`
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
        The subject name is qualified by the project id prefix, if necessary.
        
        :param project: the XNAT project id
        :param subject: the XNAT subject name
        :return: the corresponding XNAT subject (which may not exist)
        """
        # The canonical subject label.
        label = canonical_label(project, subject)
        # The query path.
        qpath = XNAT.SUBJECT_QUERY_FMT % (project, label)
        
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
        label = canonical_label(project, subject, session)
        
        return self.get_subject(project, subject).experiment(label)
    
    def get_scan(self, project, subject, session, scan):
        """
        Returns the XNAT scan object for the given XNAT lineage.
        
        See :meth:`get_session`
        
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
        The lineage names are qualified by a prefix, if necessary.
        
        See :meth:`get_session`
        
        :param project: the XNAT project id
        :param subject: the subject name
        :param session: the session name
        :param recon: the XNAT reconstruction name
        :return: the corresponding XNAT reconstruction object (which may not exist)
        """
        label = canonical_label(project, subject, session, recon)
        
        return self.get_session(project, subject, session).reconstruction(label)
      
    def get_assessor(self, project, subject, session, analysis):
        """
        Returns the XNAT assessor object for the given XNAT lineage.
        The lineage names are qualified by a prefix, if necessary.
        
        See :meth:`get_session`
        
        :param project: the XNAT project id
        :param subject: the subject name
        :param session: the session name
        :param analysis: the analysis name
        :return: the corresponding XNAT assessor object (which may not exist)
        """
        label = canonical_label(project, subject, session, analysis)
        
        return self.get_session(project, subject, session).assessor(label)
    
    # Define the get_assessor function aliases.
    get_assessment = get_assessor
    get_analysis = get_assessor
    
    def download(self, project, subject, session, **opts):
        """
        Downloads the files for the specfied XNAT session.
        
        The keyword options include the format and session child container.
        The session child container option can be set to a specific resource container,
        e.g. ``scan=1``, as described in :meth:`XNAT.upload` or all resources of a given
        container type. In the latter case, the ``container_type`` parameter is set.
        The permissible container types are described in :meth:`XNAT.upload`.
        
        The session value is qualified by the subject, if necessary.
        A reconstruction or analysis option value is qualified by the session label,
        if necessary. For example::
            
            download('QIN', 'Breast001', 'Session03', reconstruction=>'reg_jA4K')
        
        downloads the NiFTI files for the XNAT session with label ``Breast001_Session03``
        and reconstruction label ``Breast001_Session03_reg_jA4K``.
        
        :param project: the XNAT project id
        :param subject: the XNAT subject label
        :param session: the XNAT experiment label
        :param opts: the resource selection option
        :keyword format: the image file format (``NIFTI`` or ``DICOM``, default ``NIFTI``)
        :keyword scan: the scan number
        :keyword reconstruction: the reconstruction name
        :keyword analysis: the analysis name
        :keyword container_type: the container type, if no specific container is specified
        :keyword inout: the ``in``/``out`` reconstruction resource qualifier
            (default ``out``)
        :keyword dest: the optional download location (default current directory)
        :return: the downloaded file names
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
        logger.debug("Downloading the XNAT file %s to %s..." % (fname, dest))
        file_obj.get(tgt)
        logger.debug("Downloaded the XNAT file %s." % tgt)
        
        return tgt
    
    def upload(self, project, subject, session, *in_files, **opts):
        """
        Imports the given files into the XNAT resource with the following hierarchy:
    
            /project/PROJECT/subject/SUBJECT/experiment/SESSION/CTR_TYPE/CONTAINER/resource/RESOURCE
    
        where:
        
        -  the XNAT experiment name is the ``session`` parameter
        
        -  CTR_TYPE is the experiment child type, e.g. ``scan``
        
        -  the default RESOURCE is the file format, e.g. ``NIFTI`` or ``DICOM``
        
        The keyword options include the session child container, scan ``modality``,
        ``resource`` name and file ``format``. The required container keyword argument associates
        the container type to the container name, e.g. ``scan=1``. The container type is ``scan``,
        ``reconstruction`` or ``analysis``. The ``analysis`` container type value corresponds to
        the XNAT ``assessor`` Image Assessment type. ``analysis``, ``assessment`` and ``assessor``
        are synonymous. The container name can be a string or integer, e.g. the scan number.
        
        If the XNAT file extension is ``.nii``, ``.nii.gz``, ``.dcm`` or ``.dcm.gz``, then the
        default XNAT image format is inferred from the extension.
        
        If the session does not yet exist as a XNAT experiment, then the modality keyword argument
        is required. The modality is any supported XNAT modality, e.g. ``MR`` or  or ``CT``. A
        capitalized modality value is a synonym for the XNAT session data type, e.g. ``MR`` is a
        synonym for ``xnat:mrSessionData``.
        
        Example:
    
        >>> from qipipe.helpers import xnat_helper
        >>> xnat = xnat_helper.facade()
        >>> xnat.upload(project(), 'Sarcoma003', 'Sarcoma003_Session01', scan=4, modality='MR',
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
        :keyword resource: the resource name (default is the format)
        :keyword overwrite: flag indicating whether to replace an existing file (default False)
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
        if not exp.exists():
            if modality:
                if not modality.startswith('xnat:'):
                    if not modality.endswith('SessionData'):
                        if modality.isupper():
                            modality = modality.lower()
                        modality = modality + 'SessionData'
                    modality = 'xnat:' + modality
                opts['experiments'] = modality
            logger.debug("Creating the XNAT %s experiment..." % session)
            exp.insert()
            logger.debug("Created the XNAT experiment %s." % session)

        # Make the resource parent container, if necessary.
        ctr_type, ctr_id = self._infer_resource_container(opts)
        ctr = self._xnat_resource_parent(exp, ctr_type, ctr_id)
        if not ctr.exists():
            logger.debug("Creating the XNAT %s %s resource parent container %s..." %
                (session, ctr_type, ctr_id))
            ctr.insert()
            logger.debug("Created the XNAT %s %s resource parent container with id %s." %
                (session, ctr_type, ctr.id()))

        format = opts.get('format')
        # Infer the format, if necessary.
        if not format:
            format = self._infer_format(*in_files)
            if format:
                opts['format'] = format

        # The name of the resource that will hold the files. The default is the image format.
        rsc = opts.pop('resource', format)
        if not rsc:
            raise XNATError("XNAT %s upload cannot infer the image format"
                " as the default resource name" % session)
        rsc_obj = self._xnat_child_resource(ctr, rsc, opts.pop('inout', None))
        # Make the resource, if necessary.
        if not rsc_obj.exists():
            logger.debug("Creating the XNAT %s %s %s resource..." % (session, ctr_id, rsc))
            rsc_obj.insert()
            logger.debug("Created the XNAT %s %s resource with name %s and id %s." %
                (session, ctr_id, rsc_obj.label(), rsc_obj.id()))
        
        # Upload each file.
        logger.debug("Uploading the %s files to XNAT..." % session)
        xnat_files = [self._upload_file(rsc_obj, f, opts) for f in in_files]
        logger.debug("%s files uploaded to XNAT." % session)
        
        return xnat_files
    
    def _infer_xnat_resource(self, experiment, opts):
        """
        Infers the resource container item from the given options.
        
        :param experiment: the XNAT experiment object
        :param opts: the :meth:`XNAT.download` options
        :keyword format: the image file format (``NIFTI`` or ``DICOM``, default ``NIFTI``)
        :return: the container (type, value) tuple
        """
        # The image format.
        format = opts.get('format') or 'NIFTI'

        # The resource parent type and name.
        ctr_type, ctr_name = self._infer_resource_container(opts)
        # The resource parent.
        rsc_parent = self._xnat_resource_parent(experiment, ctr_type, ctr_name)
        
        # The resource.
        return self._xnat_child_resource(rsc_parent, format, opts.get('inout'))
    
    def _infer_resource_container(self, opts):
        """
        Determines the resource container item from the given options as follows:

        - If there is a ``container_type`` option, then that type is returned without a value.

        - Otherwise, if the options include a container type in :object:`XNAT.CONTAINER_TYPES`,
          then the option type and value are returned.

        - Otherwise, if the options include a container type in :object:`XNAT.ASSESSOR_SYNONYMS`,
          then the ``assessor`` container type and the option value are returned.

        - Otherwise, an exception is thrown.
        
        :param opts: the options to check
        :return: the container (type, value) tuple
        :raise XNATError: if the resource container could not be inferred
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
        :param name: the L{XNAT.CONTAINER_TYPES} or L{XNAT.ASSESSOR_SYNONYMS}
            container designator
        :return: the standard XNAT designator in L{XNAT.CONTAINER_TYPES}
        :raise XNATError: if the name does not designate a valid container type
        """
        if name in XNAT.ASSESSOR_SYNONYMS:
            return 'assessor'
        elif name in XNAT.CONTAINER_TYPES:
            return name
        else:
            raise XNATError("XNAT upload session child container not recognized: %s" % name)
    
    def _xnat_resource_parent(self, experiment, container_type, name=None):
        """
        Returns the resource parent for the given experiment and container type.
        The resource parent is the experiment child with the given container type,
        e.g a MR session scan or registration reconstruction. If there is a name,
        then the parent is the object with that name, e.g. ``reconstruction('reg_1')``.
        Otherwise, the parent is a container group, e.g. ``reconstructions``.
        
        :param experiment: the XNAT experiment
        :param container_type: the container type in L{XNAT.CONTAINER_TYPES}
        :param name: the optional container name
        :return: the XNAT resource parent object
        """
        if name:
            # Convert an integer name, e.g. scan number, to a string.
            name = str(name)
            # The parent is the session child for the given container type.
            if container_type == 'scan':
                return experiment.scan(name)
            elif container_type == 'reconstruction':
                # The recon label is prefixed by the experiment label.
                label = canonical_label(experiment.label(), name)
                return experiment.reconstruction(label)
            elif container_type == 'assessor':
                # The assessor label is prefixed by the experiment label.
                label = canonical_label(experiment.label(), name)
                return experiment.assessor(label)
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
        :param name: the resource name, e.g. ``NIFTI``
        :param inout: the container in/out option described in :meth:`XNAT.download`
            (default ``out`` for a container type that requires this option)
        :return: the XNAT resource object
        :raise XNATError: if the inout option is invalid
        """
        if name:
            if self._is_inout_container(parent):
                if inout == 'in':
                    rsc = parent.in_resource(name)
                elif inout in ['out', None]:
                    rsc = parent.out_resource(name)
                else:
                    raise XNATError("Unsupported resource inout option: %s" % inout)
                return rsc
            else:
                return parent.resource(name)
        elif _is_inout_container(parent):
            if inout == 'in':
                return parent.in_resources()
            elif inout in ['out', None]:
                return parent.out_resources()
            else:
                raise XNATError("Unsupported resource inout option: %s" % inout)
        else:
            return parent.resources()
    
    def _is_inout_container(self, container):
        """
        :param obj: the XNAT container object
        :return: whether the container resources are qualified as input or output
        """
        for ctr_type in XNAT.INOUT_CONTAINER_TYPES:
            if isinstance(container, ctr_type):
                return True
        return False
        
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
                    raise XNATError("Upload cannot determine a format from"
                        " the heterogeneous file extensions: %s vs %f" % (fname, f))

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
        if file_obj.exists() and not opts.get('overwrite'):
            raise XNATError("The XNAT file object %s already exists in the %s %s resource" %
                (fname, rsc_ctr.id(), resource.label()))
        
        # Upload the file.
        logger.debug("Inserting the XNAT file %s into the %s %s %s resource..." %
            (fname, rsc_ctr.__class__.__name__.lower(), rsc_ctr.id(), resource.label()))
        file_obj.insert(in_file, **opts)
        logger.debug("Uploaded the XNAT file %s." % fname)
    
        return fname
