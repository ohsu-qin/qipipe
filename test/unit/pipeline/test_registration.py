import os
import re
import glob
import shutil
from nose.tools import (assert_equal, assert_is_not_none)
import nipype.pipeline.engine as pe
import qixnat

from qipipe.pipeline import registration
from ... import (ROOT, PROJECT)
from ...helpers.logging import logger
from ...helpers.name_generator import generate_unique_name
from .volume_test_base import VolumeTestBase

REG_CONF = os.path.join(ROOT, 'conf', 'registration.cfg')
"""The test registration workflow configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'registration')
"""The test results directory."""

RESOURCE = generate_unique_name(__name__)
"""The XNAT registration resource name."""


class TestRegistrationWorkflow(VolumeTestBase):
    """
    Registration workflow unit tests.

    This test exercises the registration workflow on three volumes of one visit
    in each of the Breast and Sarcoma studies.
    
    The :class:`qiprofile.pipeline.registration.RegistrationWorkflow` technique
    is set to ``mock``, which mocks the registration process by simply copying
    the input scan file to the output "realigned" file. 
    """

    def __init__(self):
        super(TestRegistrationWorkflow, self).__init__(
            logger(__name__), RESULTS, use_mask=True
        )

    def test_breast(self):
        for args in self.stage('Breast'):
            for technique in registration.TECHNIQUES:
                self._test_workflow(technique, *args)

    def test_sarcoma(self):
        for args in self.stage('Sarcoma'):
            for technique in registration.TECHNIQUES:
                self._test_workflow(technique, *args)

    def _test_workflow(self, technique, project, subject, session, scan,
                       *images):
        """
        Executes :meth:`qipipe.pipeline.registration.run` on the given
        input.

        :param technique: the built-in registration technique
        :param project: the input project name
        :param subject: the input subject name
        :param session: the input session name
        :param scan: the input scan number
        :param images: the input 3D NiFTI images to register
        """
        # Register against the first image.
        ref_0 = images[0]
        # Realign the remaining images.
        moving = images[1:]
        # The target location.
        self.dest = os.path.join(RESULTS, technique, subject, session, 'scans',
                                 str(scan), 'registration', RESOURCE)
        logger(__name__).debug("Testing the %s registration workflow on %s %s"
                               " Scan %d..." %
                               (technique, subject, session, scan))
        with qixnat.connect() as xnat:
            xnat.delete(project, subject)
            result = registration.run(technique, project, subject, session, scan,
                                      ref_0, *moving, config=REG_CONF,
                                      resource=RESOURCE, dest=self.dest,
                                      base_dir=self.base_dir)
            # Verify the result.
            try:
                self._verify_result(xnat, subject, session, scan, result)
            finally:
                xnat.delete(project, subject)

    def _verify_result(self, xnat, subject, session, scan, result):
        """
        :param xnat: the XNAT connection
        :param subject: the registration subject
        :param session: the registration session
        :param scan: the input scan number
        :param result: the meth:`qipipe.pipeline.registration.run` result
            output file paths
        """
        # Verify that the XNAT resource object was created.
        rsc = xnat.find_one(PROJECT, subject, session, scan=scan,
                            resource=RESOURCE)
        assert_is_not_none(rsc,  "The %s %s Scan %d %s XNAT registration"
                                 " resource object was not created" %
                                 (subject, session, scan, RESOURCE))

        # Verify that the registration result is accurate.
        split = (os.path.split(location) for location in result)
        out_dirs, out_files = (set(files) for files in zip(*split))
        rsc_files = set(rsc.files().get())
        assert_equal(out_dirs, set([self.dest]),
                     "The %s %s Scan %d %s XNAT registration result directory"
                      " is incorrect - expected %s, found %s" %
                      (subject, session, scan, RESOURCE, self.dest, out_dirs))
        assert_equal(out_files, rsc_files,
                     "The %s %s Scan %d %s XNAT registration result file"
                     " names are incorrect - expected %s, found %s" %
                     (subject, session, scan, RESOURCE, rsc_files, out_files))

        # Verify that the output files were created.
        dest_files = (os.path.join(self.dest, location)
                      for location in os.listdir(self.dest))
        assert_equal(set(dest_files), set(result),
                     "The %s %s Scan %d %s XNAT registration result is"
                     " incorrect: %s" %
                     (subject, session, scan, RESOURCE, result))


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
