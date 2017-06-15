import os
import shutil
import distutils
from nose.tools import (assert_equal, assert_is_not_none, assert_true)
from qiutil.ast_config import read_config
from qiutil.which import which
from qipipe.pipeline import qipipeline as qip
import qixnat
from ... import (ROOT, PROJECT, CONF_DIR)
from ...helpers.logging import logger
from ...helpers.staging import subject_sources

RESULTS = os.path.join(ROOT, 'results', 'pipeline', 'qipipeline')
"""The test results directory."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixture directory."""


class TestQIPipeline(object):
    """
    Pipeline unit tests.

    Note:: a precondition for running this test is that the environment
        variable ``QIPIPE_DATA`` is set to the DICOM source directory.
        If ``QIPIPE_DATA`` is not set, then no test cases are run and a
        log message is issued.

    Note:: this test takes app. four hours to run serially without
    modeling.
    """

    def setUp(self):
        shutil.rmtree(RESULTS, True)

    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_breast(self):
        data = os.getenv('QIPIPE_DATA')
        if data:
            # Make the input area to hold a link to the first session.
            fixture = os.path.join(RESULTS, 'data', 'breast')
            parent = os.path.join(fixture, 'BreastChemo3')
            os.makedirs(parent)
            # The session data location.
            src = os.path.join(data, 'Breast_Chemo_Study', 'BreastChemo3',
                               'Visit1')
            assert_true(os.path.exists(src), "Breast test fixture not found:"
                        " %s" % src)
            # Link the first visit input to the data.
            tgt = os.path.join(parent, 'Visit1')
            os.symlink(src, tgt)
            # Run the pipeline on the first visit.
            self._test_collection('Breast', fixture)
        else:
            logger(__name__).info('Skipping the pipeline unit Breast'
                                  ' test, since the QIPIPE_DATA environment'
                                  ' variable is not set.')

    def test_sarcoma(self):
        data = os.getenv('QIPIPE_DATA')
        if data:
            # Make the input area to hold a link to the first session.
            fixture = os.path.join(RESULTS, 'data', 'sarcoma')
            parent = os.path.join(fixture, 'Subj_1')
            os.makedirs(parent)
            # The session data location.
            src = os.path.join(data, 'Sarcoma', 'Subj_1', 'Visit_1')
            assert_true(os.path.exists(src), "Sarcoma test fixture not found:"
                        " %s" % src)
            # Link the first visit input to the data.
            tgt = os.path.join(parent, 'Visit_1')
            os.symlink(src, tgt)
            # Run the pipeline on the first visit.
            self._test_collection('Sarcoma', fixture)
        else:
            logger(__name__).info('Skipping the pipeline unit Sarcoma'
                                  ' test, since the QIPIPE_DATA environment'
                                  ' variable is not set.')

    def _test_collection(self, collection, fixture):
        """
        Run the pipeline on the given collection and verify that scans are
        created in XNAT.

        :param collection: the image collection name
        :param fixture: the test input directory holding a link to the
            first visit data
        """
        logger(__name__).debug("Testing the pipeline on %s..." % fixture)

        # The staging destination and work area.
        dest = os.path.join(RESULTS, 'data')

        # The pipeline options.
        opts = dict(base_dir=RESULTS, config_dir=CONF_DIR, dest=dest,
                    project=PROJECT, collection=collection,
                    registration_technique='mock',
                    modeling_technique='mock')
        
        # The {test subject: input directory} dictionary.
        sbj_dir_dict = subject_sources(collection, fixture)
        # The test subjects.
        subjects = sbj_dir_dict.keys()
        # The test subject input directories.
        sources = sbj_dir_dict.values()

        with qixnat.connect() as xnat:
            # Delete any existing test subjects.
            for sbj in subjects:
                xnat.delete(PROJECT, sbj)
            # Run the workflow on the subject input directories.
            logger(__name__).debug("Executing the pipeline on %s..." %
                                   sbj_dir_dict)
            # The {subject: {session: }}
            output_dict = qip.run(*sources, **opts)
            # Verify the result.
            for sbj, sess_dict in output_dict.iteritems():
                for sess, results in sess_dict.iteritems():
                    # If registration is enabled, then verify the
                    # registration resource.  Otherwise, skip the
                    # remaining stage verification.
                    if actions and not 'register' in opts['actions']:
                        continue
                    # The XNAT registration resource name.
                    rsc = results['registration']
                    assert_is_not_none(rsc,
                                       "The %s %s result does not have a"
                                       " registration resource" %
                                       (sbj, sess))
                    reg_obj = xnat.get_resource(
                        PROJECT, sbj, sess, resource=rsc)
                    assert_true(reg_obj.exists(),
                                "The %s %s registration resource %s was not"
                                " created in XNAT" % (sbj, sess, rsc))
                    # If modeling is enabled, then verify the modeling resource.
                    if opts['modeling'] != False:
                        rsc = results['modeling']
                        mdl_obj = xnat.get_resource(PROJECT, sbj, sess, rsc)
                        assert_true(mdl_obj.exists(),
                                    "The %s %s modeling resource %s was not"
                                    " created in XNAT" % (sbj, sess, rsc))

            # Delete the test subjects.
            for sbj in subjects:
                xnat.delete(PROJECT, sbj)


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
