import os
import tempfile
import logging
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import (IdentityInterface, Function)
from nipype.interfaces.ants import AverageImages
from .. import project
from ..interfaces import XNATUpload
from .workflow_base import WorkflowBase
from ..helpers.logging_helper import logger


REFreg_obj = 'ref'
"""The XNAT reference reconstruction name."""


def run(input_dict, **opts):
    """
    Creates a :class:`qipipe.pipeline.reference.ReferenceWorkflow`
    and runs it on the given inputs.
    
    :param input_dict: the :meth:`qipipe.pipeline.reference.ReferenceWorkflow.run`
        inputs
    :param opts: the :class:`qipipe.pipeline.reference.ReferenceWorkflow`
        initializer and :meth:`qipipe.pipeline.reference.ReferenceWorkflow.run`
        options
    :return: the :meth:`qipipe.pipeline.reference.ReferenceWorkflow.run`
        result
    """
    return ReferenceWorkflow(**opts).run(input_dict)


class ReferenceWorkflow(WorkflowBase):

    """
    The ReferenceWorkflow class builds and executes the reference workflow.
    The workflow creates a reference image by averaging the middle images.
    
    The reference workflow input is the *input_spec* node consisting of the
    following input fields:
    
    - *subject*: the subject name
    
    - *session*: the session name
    
    - *images*: the session images
    
    The reference workflow output is the *output_spec* node consisting of
    the following output fields:
    
    - *reference*: the reference image files=
    
    The optional workflow configuration file can contain overrides for the
    Nipype interface inputs in the following sections:
    
    - ``ants.AverageImages``: the ANTS `Average interface`_ options
    
    .. _Average interface: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.ants.utils.html
    """

    def __init__(self, **opts):
        """
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.
        
        :param opts: the following initialization options:
        :keyword base_dir: the workflow execution directory
            (default a new temp directory)
        :keyword cfg_file: the optional workflow inputs configuration file
        :keyword technique: the case-insensitive workflow technique
            (``ANTS`` or ``FNIRT``, default ``ANTS``)
        """
        super(ReferenceWorkflow, self).__init__(logger(__name__),
                                                opts.pop('cfg_file', None))

        self.workflow = self._create_workflow(**opts)
        """The reference workflow."""
    
    def run(self, input_dict):
        """
        Runs the reference workflow on the scan NiFTI files for the given
        (subject, session) inputs.
        
        :param input_dict: the input *{subject: {session: [images]}}* dictionary
        :return: the reference XNAT reconstruction name
        """
        sbj_cnt = len(input_dict)
        sess_cnt = sum(map(len, input_dict.values()))
        self._logger.debug("Creating the reference images for %d sessions"
                           " from %d subjects..." % (sess_cnt, sbj_cnt))
        for sbj, sess_dict in input_dict.iteritems():
            self._logger.debug("Creating the subject %s reference images..." % sbj)
            for sess, images in sess_dict.iteritems():
                self._logger.debug("Creating the %s %s reference image..." %
                                   (sbj, sess))
                self._create_session_reference(sbj, sess, images)
                self._logger.debug("Created the %s %s reference image." %
                                   (sbj, sess))
            self._logger.debug("Created the subject %s reference images." % sbj)
        self._logger.debug("Masked %d sessions from %d subjects." %
                           (sess_cnt, sbj_cnt))
        
        # Execute the workflow.
        self._run_workflow(self.workflow)
        
        # Return the reference XNAT reconstruction name.
        return REFreg_obj
    
    def _create_session_reference(self, subject, session, images):
        # Set the inputs.
        input_spec = self.workflow.get_node('input_spec')
        input_spec.inputs.subject = subject
        input_spec.inputs.session = session
        input_spec.inputs.images = images
        
        # Execute the workflow.
        self._run_workflow(self.workflow)

    def _create_workflow(self, base_dir=None):
        """
        Creates the reference workflow.
        
        :param base_dir: the workflow execution directory
            (default is a new temp directory)
        :return: the Workflow object
        """
        self._logger.debug("Creating the reference workflow...")

        # The workflow.
        if not base_dir:
            base_dir = tempfile.mkdtemp()
        ref_wf = pe.Workflow(name='reference', base_dir=base_dir)

        # The workflow input.
        in_fields = ['subject', 'session', 'images']
        input_spec = pe.Node(IdentityInterface(fields=in_fields),
                             name='input_spec')

        # Make the reference image.
        average = pe.Node(AverageImages(), name='average')
        # The average is taken over the middle three images.
        ref_wf.connect(input_spec, ('images', _middle, 3), average, 'images')

        # Upload the reference image to XNAT.
        upload_ref_xfc = XNATUpload(project=project(), resource=REFreg_obj)
        upload_ref = pe.Node(upload_ref_xfc, name='upload_ref')
        ref_wf.connect(input_spec, 'subject', upload_ref, 'subject')
        ref_wf.connect(input_spec, 'session', upload_ref, 'session')
        ref_wf.connect(average, 'output_average_image',
                       upload_ref, 'in_files')

        # The workflow output is the reference file.
        output_spec = pe.Node(IdentityInterface(fields=['reference']),
                              name='output_spec')
        ref_wf.connect(average, 'output_average_image',
                       output_spec, 'reference')

        self._configure_nodes(ref_wf)

        self._logger.debug("Created the %s workflow." % ref_wf.name)
        # If debug is set, then diagram the workflow graph.
        if self._logger.level <= logging.DEBUG:
            self._depict_workflow(ref_wf)

        return ref_wf


def _middle(items, proportion_or_length):
    """
    Returns a sublist of the given items determined as follows:
    
    - If *proportion_or_length* is a float, then the middle fraction
        given by that parameter
    
    - If *proportion_or_length* is an integer, then the middle
        items with length given by that parameter
    
    :param items: the list of items to subset
    :param proportion_or_length: the fraction or number of middle items
        to select
    :return: the middle items
    """
    if proportion_or_length < 0:
        raise ValueError("The _middle proportion_or_length parameter is not"
                         " a non-negative number: %s" % proportion_or_length)
    elif isinstance(proportion_or_length, float):
        if proportion_or_length > 1:
            raise ValueError("The _middle proportion parameter cannot"
                             " exceed 1.0: %s" % proportion_or_length)
        offset = int(len(items) * (proportion_or_length / 2))
    elif isinstance(proportion_or_length, int):
        length = min(len(items), proportion_or_length)
        offset = int((len(items) - length) / 2)
    else:
        raise ValueError("The _middle proportion_or_length parameter is not"
                         " a number: %s" % proportion_or_length)

    return sorted(items)[offset:len(items) - offset]
