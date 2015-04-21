import os
import shutil
import distutils
from nose.tools import (assert_equal, assert_is_not_none, assert_true)
from qipipe.pipeline import qipipeline as qip
import qixnat
from qipipe.staging import airc_collection as airc
from qiutil.ast_config import read_config
from ... import (project, ROOT)
from ...helpers.logging import logger
from ...helpers.staging import subject_sources
from ...unit.pipeline.test_mask import MASK_CONF
from ...unit.pipeline.test_registration import REG_CONF
from ...unit.pipeline.test_modeling import MODELING_CONF

REG_CONF = os.path.join(ROOT, 'conf', 'registration.cfg')
"""The test registration configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'qipipeline')
"""The test results directory."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixture directory."""


class TestQIPipeline(object):

    """
    QIN Pipeline unit tests.
    
    Note:: a precondition for running this test is that the environment
        variable ``QIN_DATA`` is set to the AIRC ``HUANG_LAB`` mount point.
        If ``QIN_DATA`` is not set, then no test cases are run and a
        log message is issued.
    
    Note:: the modeling workflow is only executed if the ``fastfit``
        executable is found.
    
    Note:: this test takes app. four days to run serially without modeling.
    """

    def setUp(self):
        shutil.rmtree(RESULTS, True)

    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_breast_scans(self):
        data = os.getenv('QIN_DATA')
        if data:
            fixture = os.path.join(RESULTS, 'data', 'breast')
            parent = os.path.join(fixture, 'BreastChemo1')
            os.makedirs(parent)
            src = os.path.join(data, 'Breast_Chemo_Study', 'BreastChemo3',
                               'Visit1')
            assert_true(os.path.exists(src), "Breast test fixture not found:"
                        " %s" % src)
            dest = os.path.join(parent, 'Visit1')
            os.symlink(src, dest)
            self._test_collection('Breast', fixture)
        else:
            logger(__name__).info("Skipping the QIN pipeline unit Breast"
                                  " test, since the QIN_DATA environment"
                                  " variable is not set.")

    def test_sarcoma_scans(self):
        data = os.getenv('QIN_DATA')
        if data:
            fixture = os.path.join(RESULTS, 'data', 'sarcoma')
            parent = os.path.join(fixture, 'Subj_1')
            os.makedirs(parent)
            src = os.path.join(data, 'Sarcoma', 'Subj_1', 'Visit_1')
            assert_true(os.path.exists(src), "Sarcoma test fixture not found:"
                        " %s" % src)
            dest = os.path.join(parent, 'Visit_1')
            os.symlink(src, dest)
            self._test_collection('Sarcoma', fixture)
        else:
            logger(__name__).info("Skipping the QIN pipeline unit Sarcoma"
                                  " test, since the QIN_DATA environment"
                                  " variable is not set.")

    def _test_collection_scans(self, collection, fixture):
        """
        Run the pipeline on the given collection and verify that scans are
        created in XNAT.
        
        :param collection: the AIRC collection name
        :param fixture: the test input
        """
        logger(__name__).debug("Testing the QIN pipeline on %s..." % fixture)

        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'data')
        base_dir = os.path.join(RESULTS, 'work')

        # The pipeline options.
        opts = dict(base_dir=base_dir, dest=dest, mask=MASK_CONF,
                    registration=REG_CONF)
        # If fastfit is not available, then only execute the staging and
        # registration workflows. Otherwise, execute all workflows.
        if not distutils.spawn.find_executable('fastfit'):
            opts['actions'] = ['stage', 'register']

        # The test subject => directory dictionary.
        sbj_dir_dict = subject_sources(collection, fixture)
        # The test subjects.
        subjects = sbj_dir_dict.keys()
        # The test source directories.
        sources = sbj_dir_dict.values()

        with qixnat.connect() as xnat:
            # Delete any existing test subjects.
            xnat.delete_subjects(project(), *subjects)

            # Run the staging, mask and registration workflows, but not
            # the modeling.
            logger(__name__).debug("Executing the QIN pipeline...")
            output_dict = qip.run(collection, *sources, **opts)
            # Verify the result.
            recon = qip
            for sbj, sess_dict in output_dict.iteritems():
                for sess, results in sess_dict.iteritems():
                    if opts['mask'] == False:
                        continue
                    # Verify the registration resource.
                    if opts['registration'] == False:
                        continue
                    # The XNAT registration resource name.
                    rsc = results['registration']
                    assert_is_not_none(rsc, 
                                       "The %s %s result does not have a"
                                       " registration resource" %
                                       (sbj, sess))
                    reg_obj = xnat.get_resource(
                        project(), sbj, sess, resource=rsc)
                    assert_true(reg_obj.exists(),
                                "The %s %s registration resource %s was not"
                                " created in XNAT" % (sbj, sess, rsc))
                    # Verify the modeling resource.
                    if opts['modeling'] != False:
                        rsc = results['modeling']
                        mdl_obj = xnat.get_resource(project(), sbj, sess, rsc)
                        assert_true(mdl_obj.exists(),
                                    "The %s %s modeling resource %s was not"
                                    " created in XNAT" % (sbj, sess, rsc))

            # Delete the test subjects.
            xnat.delete_subjects(project(), *subjects)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
