import os
import glob
import shutil
from collections import defaultdict
from nose.tools import (assert_equal, assert_not_equal)
from qiutil.collections import nested_defaultdict
from ... import PROJECT
from ...helpers.logging import logger


class VolumeTestBase(object):
    """
    Base class for testing image volume workflows. The test fixture is
    a directory in the standard staging input subject/session/images
    hierarchy found in the ``test/fixtures/staging`` directory.
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
        self.base_dir = os.path.join(self._results, 'work')
        self._use_mask = use_mask

    def setUp(self):
        shutil.rmtree(self._results, True)

    def tearDown(self):
        shutil.rmtree(self._results, True)

    def stage(self, collection):
        """
        Acquires the test fixture inputs to run the workflow. This
        generator method yields the workflow input arguments,
        consisting of the subject, session and scan names followed
        by the image file names.

        :param collection: the collection name
        :param opts: additional workflow options
        :yield: the workflow inputs list
        """
        # The fixture is the collection subdirectory.
        fixture = os.path.join(self._fixtures, collection.lower())
        # The staging inputs.
        inputs = self._group_files(fixture)
        # The test subjects.
        subjects = inputs.keys()
        # Run the workflow in the results work subdirectory.
        cwd = os.getcwd()
        work_dir = os.path.join(self._results, 'work')
        os.makedirs(work_dir)
        os.chdir(work_dir)
        # Iterate over the inputs.
        try:
            return self._stage_inputs(inputs)
        finally:
            os.chdir(cwd)

    def _stage_inputs(self, inputs):
        """
        :param inputs: the :meth:`_group_files` result dictionary
        :yield: the workflow inputs list
        """
        # Iterate over the sessions within subjects.
        for sbj, sess_dict in inputs.iteritems():
            for sess, scan_dict in sess_dict.iteritems():
                for scan, opts in scan_dict.iteritems():
                    # The input images.
                    images = opts.pop('images')
                    args = [PROJECT, sbj, sess, scan] + images
                    yield args

    def _group_files(self, fixture):
        """
        Groups the files in the given test fixture directory. The fixture is a
        parent directory which contains a subject/session/scans images hierarchy.
        
        :param fixture: the input data parent directory
        :return: the input *{subject: {session: {scan: [images]}}}* or
            *{subject: {session: {scan: ([images], mask: file)}}}*
            dictionary
        """
        # The inputs dictionary return value described above.
        inputs = nested_defaultdict(dict, 2)
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
                    # The input NIFTI volume images.
                    images = glob.glob(scan_dir + '/resources/NIFTI/*')
                    assert_not_equal(len(images), 0,
                                     "No images found for %s %s %d test input in %s" %
                                     (sbj, sess, scan, scan_dir))
                    self._logger.debug("Discovered %d %s %s %d test input images in %s" %
                                       (len(images), sbj, sess, scan, scan_dir))
                    inputs[sbj][sess][scan]['images'] = images
                    # The input mask.
                    if self._use_mask:
                        masks = glob.glob(sess_dir + '/resources/*mask.*')
                        assert_not_equal(len(masks), 0, "Mask not found in %s" % sess_dir)
                        assert_equal(len(masks), 1, "Too many masks found in %s" % sess_dir)
                        mask = masks[0]
                        self._logger.debug("Discovered %d %s %s test input mask %s" %
                                           (len(images), sbj, sess, mask))
                        inputs[sbj][sess][scan]['mask'] = mask

        return inputs
