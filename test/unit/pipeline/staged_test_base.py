import os
import glob
import shutil
from collections import defaultdict
from qipipe.helpers import xnat_helper
from test import project


class StagedTestBase(object):

    """
    Base class for testing workflows on a test fixture directory in the
    standard staging subject/session/images hierarchy, e.g.::
        
        Breast003/
            Session01/
                scans/
                    series09.nii.gz
                    series23.nii.gz
                    ...
                breast003_session01_mask.nii.gz
    """

    def __init__(self, logger, fixtures, results, use_mask=False):
        """
        :param logger: this test case's logger
        :param fixtures: the fixtures parent directory
        :param results: the results parent directory
        :param use_mask: flag indicating whether the inputs include
            a mask with the input images
        """
        self._logger = logger
        self._fixtures = fixtures
        self._results = results
        self._use_mask = use_mask

    def setUp(self):
        shutil.rmtree(self._results, True)

    def tearDown(self):
        pass #shutil.rmtree(self._results, True)

    def _test_breast(self, **opts):
        self._test_collection('Breast', **opts)

    def _test_sarcoma(self, **opts):
        self._test_collection('Sarcoma', **opts)

    def _test_collection(self, collection, **opts):
        """
        Run the workflow and verify that the registered images are
        created in XNAT.
        
        :param collection: the collection name
        :param opts: additional workflow options
        """
        # The fixture is the collection subdirectory.
        fixture = os.path.join(self._fixtures, collection.lower())

        # The default workflow base directory.
        if 'base_dir' not in opts:
            opts['base_dir'] = os.path.join(self._results, 'work')

        # The inputs.
        input_dict = self._group_files(fixture)
        # The test subjects.
        subjects = input_dict.keys()
        with xnat_helper.connection() as xnat:
            # Delete the existing subjects.
            xnat_helper.delete_subjects(project(), *subjects)
            # Run the workflow.
            for sbj, sess_dict in input_dict.iteritems():
                for sess, sess_opts in sess_dict.iteritems():
                    sess_opts.update(opts)
                    result = self._run_workflow(fixture, sbj, sess, **sess_opts)
                    # Verify the result.
                    self._verify_result(xnat, sbj, sess, result)
            # Clean up.
            xnat_helper.delete_subjects(project(), *subjects)

    def _group_files(self, fixture):
        """
        Groups the files in the given test fixture directory. The fixture is a
        parent directory which contains a subject/session/scans images hierarchy.
        
        :param fixture: the input data parent directory
        :return: the input *{subject: {session: [images]}}* or
            input *{subject: {session: ([images], mask)}}* dictionary
        """
        # The {subject: {session: [files]}} dictionary return value.
        input_dict = defaultdict(dict)
        # Group the files in each subject subdirectory.
        for sbj_dir in glob.glob(fixture + '/*'):
            _, sbj = os.path.split(sbj_dir)
            for sess_dir in glob.glob(sbj_dir + '/Session*'):
                _, sess = os.path.split(sess_dir)
                images = glob.glob(sess_dir + '/scans/*')
                opts = dict(images=images)
                if self._use_mask:
                    masks = glob.glob(sess_dir + '/*mask.*')
                    if not masks:
                        raise ValueError("Mask not found in %s" % sess_dir)
                    if len(masks) > 1:
                        raise ValueError("Too many masks found in %s" %
                                         sess_dir)
                    opts['mask'] = masks[0]
                input_dict[sbj][sess] = opts

        return input_dict

    def _run_workflow(self, fixture, subject, session, images, **opts):
        """
        This method is implemented by subclasses to execute the subclass
        target workflow on the given inputs.
        
        :param fixture: the test fixture directory
        :param subject: the input subject
        :param session: the input session
        :param images: the input image files
        :param opts: the target workflow options
        :return: the execution result
        :raise NotImplementedError: if the class:`StagedTestBase` subclass
            does not implement this method
        """
        raise NotImplementedError("_run_workflow is a subclass responsibility")

    def _verify_result(self, xnat, subject, session, result):
        """
        This method is implemented by subclasses to verify the workflow
        execution result against the given session file inputs. The session
        file inputs parameter is a {(subject, session): *files* dictionary},
        where *files* is a list consisting of the session image files found
        in the test fixture directory.
        
        :param xnat: the :class:`qipipe.helpers.xnat_helpers.XNAT` connection
        :param subject: the input subject
        :param session: the input session
        :param result: the :meth`_run_workflow` result
        :raise NotImplementedError: if the class:`StagedTestBase` subclass
            does not implement this method
        """
        raise NotImplementedError(
            "_verify_result is a subclass responsibility")
