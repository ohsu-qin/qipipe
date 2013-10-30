import os
from contextlib import contextmanager
import pyxnat
from pyxnat.core.resources import (Experiment, Reconstruction, Reconstructions,
                                   Assessor, Assessors)
from .xnat_config import default_configuration

from .logging_helper import logger


@contextmanager
def connection():
    """
    Yields a :class:`qipipe.helpers.xnat_helper.XNAT` instance.
    The XNAT connection is closed when the outermost connection
    block finishes.

    Example:

    >>> from qipipe.helpers import xnat_helper
    >>> with xnat_helper.connection() as xnat:
    ...    sbj = xnat.get_subject('QIN', 'Breast003')

    :return: the XNAT instance
    :rtype: :class:`qipipe.helpers.xnat_helper.XNAT`
    """
    if not hasattr(connection, 'connect_cnt'):
        connection.connect_cnt = 0
    if not connection.connect_cnt:
        connection.xnat = XNAT()
    connection.connect_cnt += 1
    try:
        yield connection.xnat
    finally:
        connection.connect_cnt -= 1
        if not connection.connect_cnt:
            connection.xnat.close()


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
    >>> canonical_label('QIN', 'Breast003', 'QIN_Breast003_Session01', 'reg_pzR3tW')
    'QIN_Breast003_Session01_reg_pzR3tW'

    :param names: the object names
    :return: the corresponding XNAT label
    """
    names = list(names)
    if any((not n for n in names)):
        raise ValueError("The XNAT label name hierarchy is invalid: %s" % names)
    last = names.pop()
    if names:
        prefix = canonical_label(*names)
        if last.startswith(prefix):
            return last
        else:
            return "%s_%s" % (prefix, last)
    else:
        return last


def parse_canonical_label(label):
    """
    Returns the names for the given XNAT session
    label, as defined by :meth:`qipipe.helpers.xnat_helper.canonical_label`.

    Example:

    >>> from qipipe.helpers.xnat_helper import parse_canonical_label
    >>> parse_canonical_label('QIN_Breast003_Session01')
    >>> ('QIN', 'Breast003', 'Session01')

    :param: the XNAT session label
    :return: the name tuple
    """
    return tuple(label.split('_'))


def parse_session_label(label):
    """
    Parses the given XNAT session label into *project*, *subject* and
    *session* based on the :meth:`qipipe.helpers.xnat_helper.canonical_label`
    naming standard.

    :param label: the label to parse
    :return: the *(project, subject, session)* tuple
    """
    names = parse_canonical_label(label)
    if len(names) != 3:
        raise ValueError("The XNAT session label argument is not in"
                         " project_subject_session format: %s" % label)

    return names


def delete_subjects(project, *subjects):
    """
    Deletes the given XNAT subjects, if they exist.

    :param project: the XNAT project id
    :param subjects: the XNAT subject names
    """
    with connection() as xnat:
        for sbj in subjects:
            sbj_obj = xnat.get_subject(project, sbj)
            if sbj_obj.exists():
                sbj_obj.delete()
                logger(__name__).debug("Deleted the XNAT test subject %s." %
                                       sbj)


class XNATError(Exception):
    pass


class XNAT(object):

    """
    XNAT is a pyxnat facade convenience class. An XNAT instance is
    created in a :meth:`qipipe.helpers.xnat_helper.connection`
    context, e.g.:

    >>> from qipipe.helpers import xnat_helper
    >>> with xnat.connection() as xnat:
    >>>     sbj = xnat.get_subject('QIN', 'Sarcoma001')

    This XNAT wrapper class implements methods to access XNAT objects in
    a hierarchical name space using a labeling convention. The method
    parameters take name values which are used to build a XNAT label,
    as shown in the following example:

    +----------------+-------------+------------+-------------------------------------+
    | Class          | Name        | Id         | Label                               |
    +================+=============+============+=====================================+
    | Project        | QIN         | QIN        | QIN                                 |
    +----------------+-------------+------------+-------------------------------------+
    | Subject        | Breast003   | QIN_E00580 | QIN_Breast003                       |
    +----------------+-------------+------------+-------------------------------------+
    | Experiment     | Session01   | QIN_E00604 | QIN_Breast003_Session01             |
    +----------------+-------------+------------+-------------------------------------+
    | Scan           | 1           | 1          | 1                                   |
    +----------------+-------------+------------+-------------------------------------+
    | Reconstruction | reg_yJf93wC |  -         | QIN_Breast003_Session01_reg_yJf93wC |
    +----------------+-------------+------------+-------------------------------------+
    | Assessor       | pk_4kbEv3r  | QIN_E00868 | QIN_Breast003_Session01_pk_4kbEv3r  |
    +----------------+-------------+------------+-------------------------------------+

    The XNAT label is set by the user and required to be unique in the XNAT database,
    with the exception of the Scan object, which is unique within the scope of the
    experiment.

    The XNAT id is an opaque XNAT-generated identifier. Like the label, it is unique
    in the database, with the exception of the Scan id which is unique within the scope
    of the experiment. Oddly, XNAT does not generate a Reconstruction object id.

    In the example above, the XNAT reconstruction object is obtained as follows:

    >>> from qipipe.helpers import xnat_helper
    >>> with xnat.connection() as xnat:
    >>>     recon = xnat.get_reconstruction('QIN', 'Breast003', 'Session01',
    ...                                     'reg_yJf93wC')

    A scan NiFTI file ``series1.nii.gz`` is uploaded using the following code::

        xnat.upload('QIN', 'Breast003', 'Session01', scan=1, 'series1.nii.gz')

    Scan DICOM files require a *format* options, e.g.::

        xnat.upload('QIN', 'Breast003', 'Session01', scan=1, format=DICOM,
                    *dicom_files)
    """

    SUBJECT_QUERY_FMT = "/project/%s/subject/%s"
    """The subject query template."""

    CONTAINER_TYPES = ['scan', 'reconstruction', 'assessor']
    """The supported XNAT resource container types."""

    ASSESSOR_SYNONYMS = ['analysis', 'assessment']
    """Alternative designations for the XNAT ``assessor`` container type."""

    INOUT_CONTAINER_TYPES = [
        Reconstruction, Reconstructions, Assessor, Assessors]

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
            self._config = config_or_interface
            logger(__name__).debug("Connecting to XNAT with config %s..." %
                                   config_or_interface)
            self.interface = pyxnat.Interface(config=config_or_interface)

    def close(self):
        """Drops the XNAT connection."""
        self.interface.disconnect()
        logger(__name__).debug("Closed the XNAT connection.")

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

    def get_scans(self, project, subject, session):
        """
        Returns the XNAT scan numbers for the given XNAT lineage.
        The session name is qualified by the subject name prefix, if necessary.

        :param project: the XNAT project id
        :param subject: the XNAT subject label
        :param session: the XNAT experiment label
        :return: the session scan numbers
        """
        label = canonical_label(project, subject, session)
        exp = self.get_subject(project, subject).experiment(label)

        return [int(scan) for scan in exp.scans().get()]

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
        Downloads the files for the specified XNAT session.

        The keyword arguments include the format and session child container.
        The session child container option can be set to a specific resource container,
        e.g. ``scan=1``, as described in :meth:`XNAT.upload` or all resources of a given
        container type. In the latter case, the `container_type` parameter is set.
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
        :param opts: the following keyword options:
        :keyword format: the image file format (``NIFTI`` or ``DICOM``, default ``NIFTI``)
        :keyword scan: the scan number
        :keyword reconstruction: the reconstruction name
        :keyword analysis: the analysis name
        :keyword container_type: the container type, if no specific container is specified
            (default ``scan``)
        :keyword inout: the ``in``/``out`` container resource qualifier
            (default ``out`` for a container type that requires this option)
        :keyword file: the XNAT file name (default all files in the resource)
        :keyword dest: the optional download location (default current directory)
        :return: the downloaded file names
        """
        # The XNAT experiment, which must exist.
        exp = self.get_session(project, subject, session)
        if not exp.exists():
            raise XNATError("The XNAT download session was not found: %s" %
                            session)

        # The download location.
        dest = opts.pop('dest', None) or os.getcwd()
        if not os.path.exists(dest):
            os.makedirs(dest)

        # The XNAT file name.
        fname = opts.pop('file', None)

        if fname:
            file_clause = "%s file" % fname
        else:
            file_clause = "files"
        logger(__name__).debug("Downloading the %s %s %s %s to %s..." %
                               (subject, session, opts, file_clause, dest))

        # The resource.
        rsc = self._infer_xnat_resource(exp, opts)

        # Download the file(s).
        if fname:
            rsc_files = [rsc.file(fname)]
        else:
            rsc_files = list(rsc.files())
            if not rsc_files:
                logger(__name__).debug("The %s %s %s resource does not contain"
                                       " any files." % (subject, session, opts))

        return [self._download_file(f, dest) for f in rsc_files]

    def _download_file(self, file_obj, dest):
        """
        :param file_obj: the XNAT file object
        :param dest: the target directory
        :return: the downloaded file path
        """
        fname = file_obj.label()
        if not fname:
            raise XNATError(
                "XNAT file object does not have a name: %s" % file_obj)
        tgt = os.path.join(dest, fname)
        logger(__name__).debug("Downloading the XNAT file %s to %s..." %
                               (fname, dest))
        file_obj.get(tgt)
        logger(__name__).debug("Downloaded the XNAT file %s." % tgt)

        return tgt

    def upload(self, project, subject, session, *in_files, **opts):
        """
        Imports the given files into XNAT. The target XNAT resource has the
        following hierarchy::

            /project/PROJECT/
                subject/SUBJECT/
                    experiment/SESSION/
                        CTR_TYPE/CONTAINER/
                            resource/RESOURCE

        where:

        -  the XNAT experiment name is the *session* parameter

        -  CTR_TYPE is the experiment child type, e.g. ``scan``

        -  the default RESOURCE is the file format, e.g. ``NIFTI`` or ``DICOM``

        The keyword arguments include the session child container, scan
        *modality*, *resource* name and file *format*. The required container
        keyword argument associates the container type to the container name,
        e.g. ``scan=1``. The container type is ``scan``, ``reconstruction`` or
        ``analysis``. The ``analysis`` container type value corresponds to
        the XNAT ``assessor`` Image Assessment type. ``analysis``,
        ``assessment`` and ``assessor`` are synonymous. The container name can
        be a string or integer, e.g. the scan number.

        If the XNAT file extension is ``.nii``, ``.nii.gz``, ``.dcm`` or
        ``.dcm.gz``, then the default XNAT image format is inferred from the
        extension.

        If the session does not yet exist as a XNAT experiment, then the
        modality keyword argument is required. The modality is any supported
        XNAT modality, e.g. ``MR`` or  or ``CT``. A capitalized modality value
        is a synonym for the XNAT session data type, e.g. ``MR`` is a synonym
        for ``xnat:mrSessionData``.

        Example::

            from qipipe.helpers import xnat_helper
            with xnat_helper.connection() as xnat:
                xnat.upload(project(), 'Sarcoma003', 'Sarcoma003_Session01',
                    scan=4, modality='MR', format='NIFTI', '/path/to/image.nii')

        :param project: the XNAT project id
        :param subject: the XNAT subject name
        :param session: the session (XNAT experiment) name
        :param in_files: the input files to upload
        :param opts: the following session child container, file format, scan
            modality and optional additional XNAT file creation options:
        :keyword scan: the scan number
        :keyword reconstruction: the reconstruction name
        :keyword analysis: the analysis name
        :keyword modality: the session modality
        :keyword format: the image format
        :keyword resource: the resource name (default is the format)
        :keyword inout: the container ``in``/``out`` option
            (default ``out`` for a container type that requires this option)
        :keyword overwrite: flag indicating whether to replace an existing file
            (default False)
        :return: the new XNAT file names
        :raise XNATError: if the project does not exist
        :raise ValueError: if the session child resource container type option
            is missing
        :raise ValueError: if the XNAT experiment does not exist and the
            modality option is missing
        """
        # Validate that there is sufficient information to infer a resource
        # parent container.
        if not self._infer_resource_container(opts):
            raise ValueError("XNAT upload cannot infer the %s %s %s resource parent"
                             " from the options %s" % (project, subject, session, opts))

        # Infer the format, if necessary.
        format = opts.get('format')
        if not format:
            format = self._infer_format(*in_files)
            if format:
                opts['format'] = format

        # The default resource is the image format.
        if 'resource' not in opts:
            if not format:
                raise ValueError("XNAT %s upload cannot infer the image format"
                                 " as the default resource name" % session)
            opts['resource'] = format

        # The XNAT resource parent container.
        rsc_obj = self.find(project, subject, session, create=True, **opts)
        # If only the XNAT experiment was detected, then we don't have
        # enough information to continue.
        if isinstance(rsc_obj, Experiment):
            raise XNATError("A resource container could not be inferred from"
                            " the options %s" % opts)

        # Upload each file.
        logger(__name__).debug("Uploading %d %s %s %s files to XNAT..." %
              (len(in_files), project, subject, session))
        xnat_files = [self._upload_file(rsc_obj, f, opts) for f in in_files]
        logger(__name__).debug("%d %s %s %s files uploaded to XNAT." %
              (len(in_files), project, subject, session))

        return xnat_files

    def find(self, project, subject, session=None, **opts):
        """
        Finds the given XNAT object in the hierarchy:

            /project/PROJECT/subject/SUBJECT/experiment/SESSION/CTR_TYPE/CONTAINER

        where:

        -  the XNAT experiment name is the `session` parameter

        -  CTR_TYPE is the experiment child type, e.g. ``scan``

        If the ``create`` flag is set, then the object is created if it does not
        yet exist.

        The keyword arguments specify the session child container and resource.
        The container keyword argument associates the container type to the container
        name, e.g. ``reconstruction=reg_zPa4R``. The container type is ``scan``,
        ``reconstruction`` or ``analysis``. The ``analysis`` container type value
        corresponds to the XNAT ``assessor`` Image Assessment type. ``analysis``,
        ``assessment`` and ``assessor`` are synonymous. The container name can be a
        string or integer, e.g. the scan number. The resource is specified by the
        resource keyword.

        If the session does not yet exist as a XNAT experiment and the ``create``
        option is set, then the ``modality`` keyword argument specifies a supported
        XNAT modality, e.g. ``MR`` or  or ``CT``. A capitalized modality value is
        a synonym for the XNAT session data type, e.g. ``MR`` is a synonym for
        ``xnat:mrSessionData``. The default modality is ``MR``.

        Example:

        >>> from qipipe.helpers import xnat_helper
        >>> with xnat_helper.connection() as xnat:
        ...     subject = xnat.find('QIN', 'Sarcoma003')
        ...     session = xnat.find('QIN', 'Sarcoma003', 'Session01', create=True)
        ...     scan = xnat.find('QIN', 'Sarcoma003', 'Session01', scan=4)
        ...     resource = xnat.find('QIN', 'Sarcoma003', 'Session01', scan=4,
        ...         resource='NIFTI')

        :Note: pyxnat 0.9.1 incorrectly reports that an existing XNAT assessor
        does not exist. This method assumes that an assessor exists.

        :param project: the XNAT project id
        :param subject: the XNAT subject name
        :param session: the session (XNAT experiment) name
        :param opts: the following container options:
        :keyword scan: the scan number
        :keyword reconstruction: the reconstruction name
        :keyword analysis: the analysis name
        :keyword resource: the resource name
        :keyword inout: the resource direction (``in`` or ``out``)
        :keyword modality: the session modality
        :keyword create: flag indicating whether to create the XNAT object
            if it does not yet exist
        :return: the XNAT object, if it exists, `None` otherwise
        :raise XNATError: if the project does not exist
        :raise ValueError: if the session child resource container type
            option is missing
        :raise ValueError: if the XNAT experiment does not exist and the
            modality option is missing
        """
        create = opts.pop('create', False)

        # If no session is specified, then return the XNAT subject.
        if not session:
            sbj = self.get_subject(project, subject)
            if sbj.exists():
                return sbj
            elif create:
                logger(__name__).debug("Creating the XNAT %s %s subject..." %
                      (project, subject))
                sbj.insert()
                logger(
                    __name__).debug("Created the XNAT %s %s subject with id %s." %
                                    (project, subject, sbj.id()))
                return sbj
            else:
                return

        # The XNAT experiment.
        exp = self.get_session(project, subject, session)

        # If there is an experiment and we are not asked for a container,
        # then return the experiment.
        # Otherwise, if create is specified, then create the experiment.
        # Otherwise, bail.
        if not exp.exists():
            if create:
                # If the experiment must be created, then we need the modality.
                modality = opts.pop('modality', 'MR')
                # The odd way pyxnat specifies the modality is the
                # experiments option.
                opts['experiments'] = self._standardize_modality(modality)
                # Create the experiment.
                logger(
                    __name__).debug("Creating the XNAT %s %s %s experiment..." %
                                    (project, subject, session))
                exp.insert()
                logger(__name__).debug("Created the XNAT %s %s %s experiment with"
                                       " id %s." % (project, subject, session, exp.id()))
            else:
                return

        # The resource parent container.
        ctr_spec = self._infer_resource_container(opts)
        # If only the session was specified, then we are done.
        if not ctr_spec:
            return exp

        # The container was specified.
        ctr_type, ctr_id = ctr_spec
        if not ctr_id:
            raise ValueError("XNAT %s %s %s %s container id is missing" %
                            (project, subject, session, ctr_type))
        ctr = self._xnat_resource_parent(exp, ctr_type, ctr_id)
        if not ctr.exists() and ctr_type != 'assessor':
            if create:
                logger(__name__).debug("Creating the XNAT %s %s %s %s %s resource parent"
                                       " container..." %
                      (project, subject, session, ctr_type, ctr_id))
                ctr.insert()
                logger(__name__).debug("Created the XNAT %s %s %s %s %s resource parent"
                                       " container with id %s." %
                      (project, subject, session, ctr_type, ctr_id, ctr.id()))
            else:
                return

        # Find the resource, if specified.
        resource = opts.get('resource')
        if not resource:
            return ctr

        rsc = self._xnat_child_resource(ctr, resource, opts.get('inout'))
        if not rsc.exists():
            if create:
                logger(
                    __name__).debug("Creating the XNAT %s %s %s %s %s %s resource..." %
                                    (project, subject, session, ctr_type, ctr_id, resource))
                rsc.insert()
                logger(__name__).debug("Created the XNAT %s %s %s %s %s %s resource with"
                                       " id %s." %
                      (project, subject, session, ctr_type, ctr_id, resource, rsc.id()))
            else:
                return

        return rsc

    def _standardize_modality(self, modality):
        """
        Examples:

        >>> from qipipe.helpers import xnat_helper
        >>> xnat_helper.connection()._standardize_modality('MR')
        xnat:mrSessionData

        >>> from qipipe.helpers import xnat_helper
        >>> xnat_helper.connection()._standardize_modality('ctSessionData')
        xnat:ctSessionData

        :param modality: the modality option described in
            :meth:`qipipe.helpers.xnat_helper.XNAT.find`
        :return: the standard XNAT modality argument
        """
        if modality.startswith('xnat:'):
            return modality
        if not modality.endswith('SessionData'):
            if modality.isupper():
                modality = modality.lower()
            modality = modality + 'SessionData'
        return 'xnat:' + modality

    def _infer_xnat_resource(self, experiment, opts):
        """
        Infers the XNAT resource type and value from the given options.
        The default resource is the ``NIFTI`` scans.

        :param experiment: the XNAT experiment object
        :param opts: the :meth:`XNAT.download` options
        :keyword format: the image file format
            (``NIFTI`` or ``DICOM``, default ``NIFTI``)
        :return: the container *(type, value)* tuple
        """
        # The image format.
        format = opts.get('format') or 'NIFTI'

        # The resource parent type and name.
        ctr_spec = self._infer_resource_container(opts)
        if ctr_spec:
            ctr_type, ctr_name = ctr_spec
        else:
            ctr_type = 'scan'
            ctr_name = None

        # The resource parent.
        rsc_parent = self._xnat_resource_parent(experiment, ctr_type, ctr_name)

        # The resource.
        return self._xnat_child_resource(rsc_parent, format, opts.get('inout'))

    def _infer_resource_container(self, opts):
        """
        Determines the resource container item from the given options as follows:

        - If there is a *container_type* option, then that type is returned without a value.

        - Otherwise, if the options include a container type in :object:`XNAT.CONTAINER_TYPES`,
          then the option type and value are returned.

        - Otherwise, if the options include a container type in :object:`XNAT.ASSESSOR_SYNONYMS`,
          then the `assessor` container type and the option value are returned.

        - Otherwise, this method returns ``None``.

        :param opts: the options to check
        :return: the container (type, value) tuple, or None if no containe was specified
        """
        if 'container_type' in opts:
            return (opts['container_type'], None)
        for t in XNAT.CONTAINER_TYPES:
            if t in opts:
                return (t, opts[t])
        for t in XNAT.ASSESSOR_SYNONYMS:
            if t in opts:
                return ('assessor', opts[t])

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
            raise XNATError("XNAT upload session child container not recognized:"
                            " %s" % name)

    def _xnat_resource_parent(self, experiment, container_type, name=None):
        """
        Returns the resource parent for the given experiment and container
        type. The resource parent is the experiment child with the given
        container type, e.g a MR session scan or registration reconstruction.
        If there is a name, then the parent is the object with that name, e.g.
        ``reconstruction('reg_1')``. Otherwise, the parent is a container group,
        e.g. ``reconstructions``.

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
        raise XNATError("XNAT resource container type not recognized: %s" %
                        container_type)

    def _xnat_child_resource(self, parent, name=None, inout=None):
        """
        :param parent: the XNAT resource parent object
        :param name: the resource name, e.g. ``NIFTI``
        :param inout: the container in/out option described in
            :meth:`qipipe.helpers.xnat_helper.XNAT.download`
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
                    raise XNATError("Unsupported resource inout option: %s" %
                                    inout)
                return rsc
            else:
                return parent.resource(name)
        elif self._is_inout_container(parent):
            if inout == 'in':
                return parent.in_resources()
            elif inout in ['out', None]:
                return parent.out_resources()
            else:
                raise XNATError(
                    "Unsupported resource inout option: %s" % inout)
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
        logger(__name__).debug("Uploading the XNAT file %s from %s..." %
              (fname, in_file))

        # The XNAT file wrapper.
        file_obj = resource.file(fname)
        # The resource parent container.
        rsc_ctr = resource.parent()
        # Check for an existing file.
        if file_obj.exists() and not opts.get('overwrite'):
            raise XNATError("The XNAT file object %s already exists in the %s"
                            " resource" % (fname, resource.label()))

        # Upload the file.
        logger(__name__).debug("Inserting the XNAT file %s into the %s %s %s"
                               " resource..." %
              (fname, rsc_ctr.__class__.__name__.lower(), rsc_ctr.id(), resource.label()))
        file_obj.insert(in_file, **opts)
        logger(__name__).debug("Uploaded the XNAT file %s." % fname)

        return fname
