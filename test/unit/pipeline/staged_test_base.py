import os
import glob
import shutil
from collections import defaultdict
import qixnat
from qiutil.collections import nested_defaultdict
from ... import project

class StagedTestBase(object):
    """
    Base class for testing workflows on a test fixture directory in the
    standard staging subject/session/images hierarchy, e.g.::
        
        Breast003/
          Session01/
            scan/
              1/
                volume001.nii.gz
                volume002.nii.gz
                ...
            resource/
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
        shutil.rmtree(self._results, True)

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

        # The {subject: session: {images: [files], mask: file}} inputs.
        input_dict = self._group_files(fixture)
        # The test subjects.
        subjects = input_dict.keys()
        with qixnat.connect() as xnat:
            # Delete the existing subjects.
            xnat.delete_subjects(project(), *subjects)
            # Iterate over the sessions within subjects.
            for sbj, sess_dict in input_dict.iteritems():
                for sess, scan_dict in sess_dict.iteritems():
                    for scan, scan_opts in scan_dict.iteritems():
                        # The input images.
                        images = scan_opts.pop('images')
                        # The session options now only includes at most the mask.
                        # Add the general workflow options to the session options.
                        scan_opts.update(opts)
                        # Run the workflow in the results work subdirectory.
                        cwd = os.getcwd()
                        work_dir = os.path.join(self._results, 'work')
                        os.makedirs(work_dir)
                        os.chdir(work_dir)
                        try:
                            result = self._run_workflow(sbj, sess, scan, *images, **scan_opts)
                        finally:
                            os.chdir(cwd)
                        # Verify the result.
                        self._verify_result(xnat, sbj, sess, scan, result)
            # Clean up.
            xnat.delete_subjects(project(), *subjects)

    def _group_files(self, fixture):
        """
        Groups the files in the given test fixture directory. The fixture is a
        parent directory which contains a subject/session/scans images hierarchy.
        
        :param fixture: the input data parent directory
        :return: the input *{subject: {session: {scan: [images]}}}* or
            input *{subject: {session: {scan: ([images], mask)}}}* dictionary
        """
        # The {subject: {session: {scan: [files]}}} dictionary return value.
        input_dict = nested_defaultdict(dict, 2)
        # Group the files in each subject subdirectory.
        for sbj_dir in glob.glob(fixture + '/*'):
            _, sbj = os.path.split(sbj_dir)
            self._logger.debug("Discovered test input subject %s in %s" %
                               (sbj, sbj_dir))
            for sess_dir in glob.glob(sbj_dir + '/Session*'):
                _, sess = os.path.split(sess_dir)
                for scan_dir in glob.glob(sess_dir + '/scans/*'):
                    _, scan_s = os.path.split(scan_dir)
                    scan = int(scan_s)
                    images = glob.glob(scan_dir + '/resources/NIFTI/*')
                    if not images:
                        raise ValueError("No images found for %s %s %d test input in %s"
                                         (sbj, sess, scan, scan_dir))
                    self._logger.debug("Discovered %d %s %s %d test input images in %s" %
                                       (len(images), sbj, sess, scan, scan_dir))
                    input_dict[sbj][sess][scan]['images'] = images
                    if self._use_mask:
                        masks = glob.glob(sess_dir + '/resources/*mask.*')
                        if not masks:
                            raise ValueError("Mask not found in %s" % sess_dir)
                        if len(masks) > 1:
                            raise ValueError("Too many masks found in %s" % sess_dir)
                        mask = masks[0]
                        self._logger.debug("Discovered %d %s %s test input mask %s" %
                                           (len(images), sbj, sess, mask))
                        input_dict[sbj][sess][scan]['mask'] = mask

        return input_dict

    def _run_workflow(self, subject, session, scan, *images, **opts):
        """
        This method is implemented by subclasses to execute the subclass
        target workflow on the given inputs.
        
        :param subject: the input subject
        :param session: the input session
        :param scan: the input scan number
        :param images: the input image files
        :param opts: the target workflow options
        :return: the execution result
        :raise NotImplementedError: if the class:`StagedTestBase` subclass
            does not implement this method
        """
        raise NotImplementedError("_run_workflow is a subclass responsibility")

    def _verify_result(self, xnat, subject, session, scan, result):
        """
        This method is implemented by subclasses to verify the workflow
        execution result against the given session file inputs. The session
        file inputs parameter is a {(subject, session): *files* dictionary},
        where *files* is a list consisting of the session image files found
        in the test fixture directory.
        
        :param xnat: the :class:`qixnat.facade.XNAT` connection
        :param subject: the input subject
        :param session: the input session
        :param scan: the input scan number
        :param result: the :meth`_run_workflow` result
        :raise NotImplementedError: if the class:`StagedTestBase` subclass
            does not implement this method
        """
        raise NotImplementedError(
            "_verify_result is a subclass responsibility")
