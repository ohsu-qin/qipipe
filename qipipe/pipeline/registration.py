import os
import re
import tempfile
import logging
# The ReadTheDocs build does not include nipype.
on_rtd = os.environ.get('READTHEDOCS') == 'True'
if not on_rtd:
    # Disable nipype nipy import FutureWarnings.
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        from nipype.pipeline import engine as pe
        from nipype.interfaces.utility import (
            IdentityInterface, Function, Merge
        )
        from nipype.interfaces.ants import (
            AverageImages, Registration, ApplyTransforms
        )
        from nipype.interfaces import fsl
        from nipype.interfaces.dcmstack import (MergeNifti, CopyMeta)
import qiutil
from ..helpers.logging import logger
from ..helpers.constants import VOLUME_FILE_PAT
from ..helpers import bolus_arrival
from ..interfaces import (StickyIdentityInterface, Copy, XNATUpload)
from ..interfaces.ants import AffineInitializer
from .workflow_base import WorkflowBase
from .pipeline_error import PipelineError

REG_PREFIX = 'reg_'
"""The XNAT registration resource name prefix."""

REG_SCAN_WF_NAME = 'register_scan'

REG_IMAGE_WF_NAME = 'register_image'

DEF_TECHNIQUE = 'ANTs'

ANTS_CONF_SECTIONS = ['ants.Registration']
"""The common ANTs registration configuration sections."""

ANTS_INITIALIZER_CONF_SECTION = 'ants.AffineInitializer'
"""The initializer ANTs registration configuration sections."""

FSL_CONF_SECTIONS = ['fsl.FLIRT', 'fsl.FNIRT']
"""The FSL registration configuration sections."""


def run(subject, session, scan, in_files, **opts):
    """
    Runs the registration workflow on the given session scan images.

    :param subject: the subject name
    :param session: the session name
    :param scan: the scan number
    :param in_files: the input session scan 3D NIfTI images
    :param opts: the :class:`RegisterScanWorkflow` initializer
        and :meth:`RegisterScanWorkflow.run` options as well
        as the following keyword option:
    :keyword reference: the volume number of the image to register
         against (default is the first image)
    :return: the 4D registration time series
    """
    # The fixed reference volume number.
    ref_vol_nbr = opts.pop('reference', 1)
    # The input scan files sorted by volume number.
    volumes = sorted(in_files, key=_extract_volume_number)
    # The initial fixed image.
    ref_ndx = ref_vol_nbr - 1
    reference = volumes[ref_ndx]
    # The images to register.
    non_ref_vols = volumes[:ref_ndx] + volumes[ref_vol_nbr:]

    # The mask option is a run parameter.
    mask = opts.pop('mask', None)
    # Make the workflow.
    workflow = RegisterScanWorkflow(reference=reference, **opts)
    # Execute the workflow.
    time_series = workflow.run(subject, session, scan, non_ref_vols, mask)

    # Return the registration result 4D time series.
    return time_series


def _extract_volume_number(in_file):
    """
    :param in_file: the 3D NIfTI volume file
    :return: the volume number
    :raise PipelineError: if the file base name does not match the
        :const:`qipipe.helpers.VOLUME_FILE_PAT` pattern
    """
    _, base_name = os.path.split(in_file)
    match = VOLUME_FILE_PAT.match(base_name)
    if not match:
        raise PipelineError(
            "The volume file base name %s does not match the pattern %s" %
            (base_name, VOLUME_FILE_PAT.pattern)
        )

    return int(match.group('volume_number'))


class RegisterScanWorkflow(WorkflowBase):
    """
    The RegistrationWorkflow registers input NIfTI scan images against
    a reference image.

    The mask can be obtained by running the
    :class:`qipipe.pipeline.mask.MaskWorkflow` workflow.

    Three registration techniques are supported:

    - ``ants``: ANTS_ SyN_ symmetric normalization diffeomorphic
      registration (default)

    - ``fsl``: FSL_ FNIRT_ non-linear registration

    - ``mock``: Test technique which copies each input scan image to
      the output image file

    The optional workflow configuration file can contain overrides for
    the Nipype interface inputs in the following sections:

    - ``AffineInitializer``: the
       :class:`qipipe.interfaces.ants.utils.AffineInitializer` options

    - ``ants.Registration``: the ANTs `Registration interface`_ options

    - ``ants.ApplyTransforms``: the ANTs `ApplyTransform interface`_
      options

    - ``fsl.FNIRT``: the FSL `FNIRT interface`_ options

    .. Note:: Since the XNAT *resource* name is unique, a
        :class:`qipipe.pipeline.registration.RegisterScanWorkflow`
        instance can be used for only one registration workflow.
        Different registration inputs require different
        :class:`qipipe.pipeline.registration.RegisterScanWorkflow`
        instances.

    .. _ANTS: http://stnava.github.io/ANTs/
    .. _ApplyTransform interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.resampling.html
    .. _FNIRT: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FNIRT#Research_Overview
    .. _FNIRT interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.fsl.preprocess.html
    .. _FSL: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FSL
    .. _Registration interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.registration.html
    .. _SyN: http://www.ncbi.nlm.nih.gov/pubmed/17659998
    """

    def __init__(self, reference, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param reference: the volume to register against
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            and :class:`RegisterImageWorkflow` options, as well as the
            following keyword arguments:
        :keyword technique: the optional registration :attr:`technique`
            (default :const:`DEF_TECHNIQUE`)
        """
        super(RegisterScanWorkflow, self).__init__(__name__, **opts)

        # The reference image file path.
        self.reference = reference

        # The registration technique.
        technique_opt = opts.pop('technique', None)
        if technique_opt:
            technique = technique_opt
        else:
            technique = DEF_TECHNIQUE
            self.logger.debug("Registering with the default technique %s" %
                              technique)
        self.technique = technique.lower()

        # Make the XNAT resource name.
        """
        The registration technique (default :const:`DEF_TECHNIQUE`).
        """
        self.resource = REG_PREFIX + qiutil.file.generate_file_name()

        """
        The unique XNAT registration resource name. Uniqueness permits
        more than one registration to be stored for a given session
        without a name conflict.
        """

        self.workflow = self._create_workflow(**opts)
        """The registration workflow."""

    def run(self, subject, session, scan, in_files, mask=None):
        """
        Runs the registration workflow on the given session scan images.

        :param subject: the subject name
        :param session: the session name
        :param scan: the scan number
        :param in_files: the input session scan volume image files
        :param mask: the optional image mask file path
        :return: the realigned 4D time series file path
        """
        # Set the execution workflow inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.scan = scan
        if mask:
            input_spec.inputs.mask = mask

        # Iterate over the input images.
        iter_input = self.workflow.get_node('iter_input')
        iter_input.iterables = ('in_file', in_files)

        # Execute the workflow.
        self.logger.debug(
            "Registering %d %s %s images against the reference image"
            " %s..." % (len(in_files), subject, session, self.reference)
        )
        wf_res = self._run_workflow()
        # If dry_run is set, then there is no result.
        if not wf_res:
            return None

        # The magic incantation to get the Nipype workflow result.
        output_res = next(n for n in wf_res.nodes() if n.name == 'output_spec')
        time_series = output_res.inputs.get()['time_series']
        self.logger.debug(
            "Registered %d %s %s scan %d images as time series %s." %
            (len(in_files), subject, session, scan, time_series)
        )

        return time_series

    def _create_workflow(self, **opts):
        """
        Makes the Nipype registration workflow. The workflow input
        is the *input_spec* node consisting of the following input
        fields:

        - *subject*: the subject name

        - *session*: the session name

        - *scan*: the scan number

        - *mask*: the mask to apply to the images

        - *reference*: the fixed reference for the given image
          registration

        - *resource*: the XNAT registration resource name

        In addition, the  ``iter_input`` has the following iterable
        input fields:

        -  the 3D image files to realign

        :param reference: the initial fixed reference image
        :return: the execution workflow
        """
        # The Nipype workflow.
        self.logger.debug("Building the %s workflow..." % REG_SCAN_WF_NAME)
        workflow = pe.Workflow(name=REG_SCAN_WF_NAME, base_dir=self.base_dir)

        # The child realignment workflow.
        reg_image_wf_opts = self._child_options()
        reg_image_wf = RegisterImageWorkflow(self.technique,
                                             **reg_image_wf_opts)

        # The registration workflow input.
        input_fields = ['subject', 'session', 'scan', 'mask',
                        'reference', 'resource']
        input_spec = pe.Node(IdentityInterface(fields=input_fields),
                             name='input_spec')
        # The initial fixed reference image.
        input_spec.inputs.reference = self.reference
        # The registration resource name.
        input_spec.inputs.resource = self.resource
        # The registration mask.
        workflow.connect(input_spec, 'mask',
                         reg_image_wf.workflow, 'input_spec.mask')
        # The fixed reference image.
        workflow.connect(input_spec, 'reference',
                         reg_image_wf.workflow, 'input_spec.reference')

        # The realignment child workflow iterator.
        iter_reg_fields = ['in_file']
        iter_input = pe.Node(IdentityInterface(fields=iter_reg_fields),
                             name='iter_input')
        workflow.connect(iter_input, 'in_file',
                         reg_image_wf.workflow, 'input_spec.in_file')

        # Collect the realigned images.
        collect_realigned_xfc = IdentityInterface(fields=['realigned_files'])
        collect_realigned = pe.JoinNode(
            collect_realigned_xfc, joinsource='iter_input',
            joinfield='realigned_files', name='collect_realigned'
        )
        workflow.connect(reg_image_wf.workflow, 'output_spec.out_file',
                         collect_realigned, 'realigned_files')

        # Merge the registered images into a 4D time series.
        reg_ts_name = self.resource + '_ts'
        merge_xfc = MergeNifti(out_format=reg_ts_name)
        merge = pe.Node(merge_xfc, name='merge_volumes')
        workflow.connect(collect_realigned, 'realigned_files',
                         merge, 'in_files')

        # Make the profile.
        cr_prf_fields = ['technique', 'configuration', 'sections',
                         'reference', 'resource']
        cr_prf_xfc = Function(input_names=cr_prf_fields,
                              output_names=['out_file'],
                              function=_create_profile)
        cr_prf = pe.Node(cr_prf_xfc, name='create_profile')
        cr_prf.inputs.resource = self.resource
        cr_prf.inputs.technique = self.technique
        workflow.connect(input_spec, 'reference', cr_prf, 'reference')
        cr_prf.inputs.configuration = self.configuration
        # The profile sections depend on the technique.
        if self.technique == 'ants':
            profile_sections = ANTS_CONF_SECTIONS
            if opts.get('initialize'):
                profile_sections.append(ANTS_INITIALIZER_CONF_SECTION)
        elif self.technique == 'fsl':
            profile_sections = FSL_CONF_SECTIONS
        elif self.technique == 'mock':
            profile_sections = []
        cr_prf.inputs.sections = profile_sections
        # The profile file name.
        cr_prf.inputs.dest = "%s.cfg" % self.resource

        # Collect the fixed reference and registration result into
        # one volume list.
        collect_volumes = pe.Node(Merge(2), name='collect_volumes')
        workflow.connect(input_spec, 'reference', collect_volumes, 'in1')
        workflow.connect(collect_realigned, 'realigned_files',
                         collect_volumes, 'in2')

        # Collect the profile, volumes and time series into one list.
        collect_uploads = pe.Node(Merge(3), name='collect_uploads')
        workflow.connect(collect_volumes, 'out', collect_uploads, 'in1')
        workflow.connect(merge, 'out_file', collect_uploads, 'in2')
        workflow.connect(cr_prf, 'out_file', collect_uploads, 'in3')

        # Upload the registration result into the XNAT registration
        # resource.
        upload_xfc = XNATUpload(project=self.project, modality='MR')
        upload = pe.Node(upload_xfc, name='upload')
        workflow.connect(input_spec, 'subject', upload, 'subject')
        workflow.connect(input_spec, 'session', upload, 'session')
        workflow.connect(input_spec, 'scan', upload, 'scan')
        workflow.connect(input_spec, 'resource', upload, 'resource')
        workflow.connect(collect_uploads, 'out', upload, 'in_files')

        # The execution output is the time series.
        output_spec = pe.Node(StickyIdentityInterface(fields=['time_series']),
                              name='output_spec')
        workflow.connect(merge, 'out_file', output_spec, 'time_series')

        self.logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)

        return workflow

class RegisterImageWorkflow(WorkflowBase):
    """
    The RegisterImageWorkflow registers an input NIfTI scan image
    against a reference image.

    Three registration techniques are supported:

    - ``ants``: ANTS_ SyN_ symmetric normalization diffeomorphic
      registration (default)

    - ``fsl``: FSL_ FNIRT_ non-linear registration

    - ``mock``: Test technique which copies each input scan image to
      the output image file

    The optional workflow configuration file can contain overrides for
    the Nipype interface inputs in the following sections:

    - ``AffineInitializer``: the
       :class:`qipipe.interfaces.ants.utils.AffineInitializer` options

    - ``ants.Registration``: the ANTs `Registration interface`_ options

    - ``ants.ApplyTransforms``: the ANTs `ApplyTransform interface`_
      options

    - ``fsl.FNIRT``: the FSL `FNIRT interface`_ options

    .. Note:: Since the XNAT *resource* name is unique, a
        :class:`qipipe.pipeline.registration.RegisterScanWorkflow`
        instance can be used for only one registration workflow.
        Different registration inputs require different
        :class:`qipipe.pipeline.registration.RegisterScanWorkflow`
        instances.

    .. _ANTS: http://stnava.github.io/ANTs/
    .. _ApplyTransform interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.resampling.html
    .. _FNIRT: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FNIRT#Research_Overview
    .. _FNIRT interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.fsl.preprocess.html
    .. _FSL: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FSL
    .. _Registration interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.registration.html
    .. _SyN: http://www.ncbi.nlm.nih.gov/pubmed/17659998
    """

    def __init__(self, technique, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param technique: the required registration :attr:`technique`
        :param opts: the :class:`qipipe.pipeline.workflow_base.WorkflowBase`
            initializer options, as well as the following keyword arguments:
        :keyword initialize: flag indicating whether to create an initial
            affine transform (ANTs only, default false)
        """
        super(RegisterImageWorkflow, self).__init__(__name__, **opts)

        self.technique = technique
        """
        The lower-case XNAT registration technique. The built-in techniques
        include ``ants``, `fnirt`` and ``mock``.
        """

        self.workflow = self._create_workflow(**opts)
        """The realignment workflow."""

    def run(self, in_file, reference, **opts):
        """
        Runs the realignment workflow on the given session scan image.

        :param reference: the volume to register against
        :param in_file: the input session scan volume image file
        :param opts: the following keyword arguments:
        :option mask: the image mask file path
        :return: the realigned output file paths
        """

        # Set the workflow inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.in_file = in_file
        mask = opts.get('mask')
        if mask:
            input_spec.inputs.mask = mask

        # Execute the workflow.
        self.logger.debug("Executing the %s workflow on %s..." %
                          (self.workflow.name, in_file))
        self._run_workflow()
        self.logger.debug("Executed the %s workflow on %s." %
                          (self.workflow.name, in_file))

        # The magic incantation to get the Nipype workflow result.
        output_res = next(n for n in wf_res.nodes() if n.name == 'output_spec')
        results = output_res.inputs.get()['out_file']

        return out_file

    def _create_workflow(self, **opts):
        """
        Creates the Nipype workflow to realign the image. The
        workflow input is the *input_spec* node consisting of
        the following input fields:

        - *in_file*: the image file to realign

        - *reference*: the fixed reference image

        - *mask*: the optional mask to apply to the images

        :param opts: the following keyword arguments:
        :keyword initialize: flag indicating whether to create an initial
            affine transform (ANTs only, default false)
        :return: the Nipypye Workflow object
        """
        # The workflow.
        self.logger.debug("Building the %s image registration workflow..." %
                          self.technique)
        workflow = pe.Workflow(name=self.technique, base_dir=self.base_dir)

        # The workflow input.
        in_fields = ['in_file', 'reference', 'mask']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')

        # Copy the DICOM meta-data. The copy target is set by the
        # technique node defined below.
        copy_meta = pe.Node(CopyMeta(), name='copy_meta')
        workflow.connect(input_spec, 'in_file', copy_meta, 'src_file')

        # The input file name without directory.
        base_name_xfc = Function(input_names=['in_file'],
                                 output_names=['out_file'],
                                 function=_base_name)
        base_name = pe.Node(base_name_xfc, name='base_name')
        workflow.connect(input_spec, 'in_file', base_name, 'in_file')

        if self.technique == 'ants':
            # Nipype bug work-around:
            # Setting the registration metric and metric_weight inputs
            # after the node is created results in a Nipype input trait
            # dependency warning. Avoid this warning by setting these
            # inputs in the constructor from the values in the configuration.
            reg_cfg = self._interface_configuration(Registration)
            metric_inputs = {field: reg_cfg[field]
                             for field in ['metric', 'metric_weight']
                             if field in reg_cfg}

            # Register the images to create the rigid, affine and SyN
            # ANTS transformations. The float option is set to reduce
            # the output image size by app. 4x.
            #
            # Note: The float option has no effect. The work-around
            # is to change the data type from float to short in the
            # downsize node defined below.
            # TODO - isolate and fix this Nipype defect.
            reg_xfc = Registration(float=True, **metric_inputs)
            register = pe.Node(reg_xfc, name='register')
            workflow.connect(input_spec, 'reference', register, 'fixed_image')
            workflow.connect(input_spec, 'in_file', register, 'moving_image')
            workflow.connect(input_spec, 'mask',
                               register, 'moving_image_mask')
            workflow.connect(input_spec, 'mask',
                               register, 'fixed_image_mask')

            # If the initialize option is set, then make an initial
            # transform.
            initialize = opts.get('initialize')
            # Nipype bug work-around:
            # Setting the registration metric and metric_weight inputs
            # after the node is created results in a Nipype input trait
            # dependency warning. Avoid this warning by setting these
            # inputs in the constructor from the values in the configuration.
            reg_cfg = self._interface_configuration(Registration)
            metric_inputs = {field: reg_cfg[field]
                             for field in ['metric', 'metric_weight']
                             if field in reg_cfg}
            if initialize:
                aff_xfc = AffineInitializer()
                init_xfm = pe.Node(aff_xfc, name='initialize_affine')
                workflow.connect(input_spec, 'reference',
                                   init_xfm, 'fixed_image')
                workflow.connect(input_spec, 'in_file',
                                   init_xfm, 'moving_image')
                workflow.connect(input_spec, 'mask',
                                   init_xfm, 'image_mask')
                workflow.connect(init_xfm, 'affine_transform',
                                   register, 'initial_moving_transform')
                # Work around the following Nipype bug:
                # * If the registration has an initial_moving_transform,
                #   then the default invert_initial_moving_transform value
                #   is not applied, resulting in the following error:
                #
                #     TraitError: Each element of the 'forward_invert_flags'
                #     trait of a RegistrationOutputSpec instance must be a
                #     boolean, but a value of <undefined>
                #     <class 'traits.trait_base._Undefined'> was specified.
                #
                #   The forward_invert_flags output field is set from the
                #   invert_initial_moving_transform input field. Even
                #   though the invert_initial_moving_transform trait
                #   specifies default=False, the
                #   invert_initial_moving_transform value is apparently
                #   undefined. Perhaps the input trait should also set
                #   the usedefault option. The work-around is to explicitly
                #   set the invert_initial_moving_transform field to False.
                register.inputs.invert_initial_moving_transform = False

            # Apply the transforms to the input image.
            apply_xfm = pe.Node(ApplyTransforms(), name='apply_xfm')
            workflow.connect(input_spec, 'reference',
                               apply_xfm, 'reference_image')
            workflow.connect(input_spec, 'in_file', apply_xfm, 'input_image')
            workflow.connect(base_name, 'out_file', apply_xfm, 'output_image')
            workflow.connect(register, 'forward_transforms',
                               apply_xfm, 'transforms')

            # Work-around for Nipype bug described in the TODO comment
            # above.
            # Downsize the data type to a signed short int.
            #
            # Note: Nipype fsl.maths.ChangeDataType appends a suffix to
            # the output file and does not support an interface field
            # that changes that. The work-around work-around is to
            # make a symlink to the downsize result with the correct
            # base name.
            downsize_xfc = fsl.maths.ChangeDataType(output_datatype='short')
            downsize = pe.Node(downsize_xfc, name='downsize')
            workflow.connect(apply_xfm, 'output_image', downsize, 'in_file')
            symlink_xfc = Function(input_names=['in_file', 'link_name'],
                                   output_names=['out_file'],
                                   function=_symlink_in_place)
            symlink = pe.Node(symlink_xfc, name='restore_volume_file_name')
            workflow.connect(downsize, 'out_file', symlink, 'in_file')
            workflow.connect(base_name, 'out_file', symlink, 'link_name')
            # End of work-around.

            # Copy the meta-data.
            workflow.connect(symlink, 'out_file', copy_meta, 'dest_file')

        elif self.technique == 'fsl':
            # Make the affine transformation.
            flirt = pe.Node(fsl.FLIRT(), name='flirt')
            workflow.connect(input_spec, 'reference', flirt, 'reference')
            workflow.connect(input_spec, 'in_file', flirt, 'in_file')

            # Copy the input to a work directory, since FNIRT adds
            # temporary files to the input image location.
            fnirt_copy_moving = pe.Node(Copy(), name='fnirt_copy_moving')
            workflow.connect(input_spec, 'in_file',
                               fnirt_copy_moving, 'in_file')

            # Register the image.
            fnirt = pe.Node(fsl.FNIRT(), name='fnirt')
            workflow.connect(input_spec, 'reference', fnirt, 'ref_file')
            workflow.connect(flirt, 'out_matrix_file', fnirt, 'affine_file')
            workflow.connect(fnirt_copy_moving, 'out_file', fnirt, 'in_file')
            workflow.connect(input_spec, 'mask', fnirt, 'inmask_file')
            workflow.connect(input_spec, 'mask', fnirt, 'refmask_file')
            workflow.connect(base_name, 'out_file', fnirt, 'warped_file')

            # Copy the meta-data.
            workflow.connect(fnirt, 'warped_file', copy_meta, 'dest_file')

        elif self.technique == 'mock':
            # Copy the input scan file to an output file.
            mock_copy = pe.Node(Copy(), name='mock_copy')
            workflow.connect(input_spec, 'in_file',
                               mock_copy, 'in_file')
            workflow.connect(mock_copy, 'out_file', copy_meta, 'dest_file')
        else:
            raise PipelineError("Registration technique not recognized: %s" %
                                self.technique)

        # The output is the realigned image.
        output_spec = pe.Node(IdentityInterface(fields=['out_file']),
                              name='output_spec')
        workflow.connect(copy_meta, 'dest_file', output_spec, 'out_file')

        self._configure_nodes(workflow)

        self.logger.debug("Created the %s workflow." % workflow.name)
        # If debug is set, then diagram the workflow graph.
        if self.logger.level <= logging.DEBUG:
            self.depict_workflow(workflow)

        return workflow


### Utility functions called by the workflow nodes. ###

def _create_profile(technique, configuration, sections, reference, resource):
    """
    :meth:`qipipe.helpers.metadata.create_profile` wrapper. The
    output file base name is _resource_``.cfg``.

    :param technique: the registration technique
    :param configuration: the registration workflow interface settings
    :param sections: the profile sections
    :param reference: the fixed reference image file path
    :param resource: the registration resource name
    :return: the output profile file path
    """
    import os
    from qipipe.helpers import metadata

    # The reference is the XNAT file name without a directory.
    _, prf_reference = os.path.split(reference)
    # The correct technique names.
    TECHNIQUE_NAMES = dict(ants='ANTs', fsl='FSL', mock='Mock')
    prf_technique = TECHNIQUE_NAMES.get(technique.lower(), technique)
    # Replace the technique in configuration keys for consistency.
    tech_pat = "$%s\." % technique
    tech_sub = "%s " % prf_technique
    fix_key = lambda s: re.sub(tech_pat, tech_sub, s)
    prf_cfg = {fix_key(k): v for k, v in configuration.iteritems()}
    # The general [Registration] topic additional properties.
    reg_cfg = dict(technique=prf_technique, reference=prf_reference)
    # Update or create the [Registration] section.
    if 'registration' in prf_cfg:
        prf_cfg['registration'].update(reg_cfg)
    else:
        prf_cfg['registration'] = reg_cfg
    # The profile file name.
    base_name = "%s.cfg" % resource

    return metadata.create_profile(prf_cfg, sections, dest=base_name)


def _symlink_in_place(in_file, link_name):
    """
    Creates a symlink from *in_file* to the target *link_name*
    within the same parent directory, e.g. for current directory
    ``/a/b/``::

        >>  _symlink_in_place('c/d/src.txt', 'tgt.txt')
        "/a/b/c/d/tgt.txt"

    where the link source is ``src.txt``.

    :param in_file: the input file path
    :param link_name: the link base name
    :return: the absolute link file path
    """
    import os

    in_file = os.path.abspath(in_file)
    in_dir, in_base_name = os.path.split(in_file)
    dest_file = os.path.join(in_dir, link_name)
    os.symlink(in_base_name, dest_file)

    return dest_file


def _base_name(in_file):
    """
    :param in_file: the input file path
    :return: the file name without a directory
    """
    import os

    return os.path.split(in_file)[1]


def _copy_files(in_files, dest):
    """
    :param in_files: the input files
    :param dest: the destination directory
    :return: the output files
    """
    from qipipe.interfaces import Copy

    return [Copy(in_file=in_file, dest=dest).run().outputs.out_file
            for in_file in in_files]


def _recurse(workflow, input_nodes, output_nodes, reference):
    """
    Sets the given workflow input *reference*. The reference
    for the first input node is the *reference* file.
    The reference for each subsequent node is the prior
    registration result.

    :param workflow: the workflow delegate which connects nodes
    :param input_nodes: the iterable expansion input scan image nodes
    :param output_nodes: the iterable expansion registration output
        nodes
    :param reference: the starting reference input node
    """
    # The reference for the first node is the initial reference.
    input_nodes[0].inputs.reference = reference
    # The reference for the remaining nodes is the previous
    # registration result.
    for i in range(1, node_cnt - 1):
        workflow.connect(output_nodes[i], 'out_file',
                         input_nodes[i + 1], 'reference')
