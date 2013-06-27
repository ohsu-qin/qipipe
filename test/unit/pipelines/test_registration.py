import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from test.helpers.project import project
from qipipe.pipelines import registration
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from qipipe.helpers.ast_config import read_config

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'registration')
"""The test fixtures directory."""

REG_CONF = os.path.join(ROOT, 'conf', 'registration.cfg')
"""The test registration configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'registration')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestRegistrationWorkflow(object):
    """
    Registration workflow unit tests.
    
    This test exercises the registration workflow on three series of one visit in each of the
    Breast and Sarcoma studies.
    """
    def __init__(self):
        """Sets the ``_logger`` instance variable."""
        self._logger = logger
    
    def setUp(self):
        shutil.rmtree(RESULTS, True)
    
    def tearDown(self):
        shutil.rmtree(RESULTS, True)
    
    def test_breast(self):
        self._test_collection('Breast')
    
    def test_sarcoma(self):
        self._test_collection('Sarcoma')
    
    def _test_collection(self, collection):
        """
        Run the registration workflow and verify that the registered images are
        created in XNAT.
        
        :param collection: the collection name
        """
        fixture = os.path.join(FIXTURES, collection.lower())
        self._logger.debug("Testing the workflow on %s..." % fixture)

        # The workflow base directory.
        base_dir = os.path.join(RESULTS, collection.lower(), 'work')
        
        with xnat_helper.connection() as xnat:
            # Seed XNAT with the test files.
            sess_files_dict = self._seed_xnat(fixture, collection)
            # Run the workflow.
            recon_specs = self._run_workflow(*sess_files_dict.iterkeys(), base_dir=base_dir)
            # Verify the result.
            self._verify_result(xnat, sess_files_dict, recon_specs)
            
            # Clean up.
            subjects = {sbj for sbj, _ in sess_files_dict.iterkeys()}
            delete_subjects(project(), *subjects)

    def _seed_xnat(self, fixture, collection):
        """
        Seed XNAT with the test files.
        
        :return: the (subject, session) => scan files dictionary
        """
        sess_files_dict = {}
        for sbj_dir in glob.glob(fixture + '/' + collection + '*'):
            _, sbj = os.path.split(sbj_dir)
            # Delete a stale test XNAT subject, if necessary.
            delete_subjects(project(), sbj)
            # Populate the test XNAT subject from the test fixture.
            for sess_dir in glob.glob(sbj_dir + '/Session*'):
                sess, in_files = self._upload_session_files(sbj, sess_dir)
                sess_files_dict[(sbj, sess)] = in_files
        return sess_files_dict
        
    def _run_workflow(self, *session_specs, **opts):
        return registration.run(*session_specs, config=REG_CONF, **opts)
    
    def _verify_result(self, xnat, sess_files_dict, *recon_specs):
        sess_recon_dict = {(sbj, sess): recon for sbj, sess, recon in recon_specs}
        for spec, in_files in sess_files_dict.iteritems():
            assert_in(spec, sess_recon_dict, "The session %s %s was not registered" % spec)
            recon = sess_recon_dict[spec]
            sbj, sess = spec
            recon_obj = xnat.get_reconstruction(project(), sbj, sess, recon)
            assert_true(recon_obj.exists(),
                "The %s %s %s XNAT reconstruction object was not created" % (sbj, sess, recon))
            recon_files = recon_obj.out_resource('NIFTI').files().get()
            assert_equals(len(in_files), len(recon_files),
                "The registered %s %s file count is incorrect - expected: %d, found: %d" %
                (sbj, sess, len(in_files), len(recon_files)))
           
    def _upload_session_files(self, subject, session_dir):
        """
        Uploads the test files in the given session directory.
        
        :param subject: the test subject label
        :param session_dir: the session directory
        :return: the XNAT (session, files) labels
        """
        _, dname = os.path.split(session_dir)
        with xnat_helper.connection() as xnat:
            sess_obj = xnat.get_session(project(), subject, dname)
            fnames = [self._upload_file(sess_obj, f) for f in glob.glob(session_dir + '/series*.nii.gz')]
            self._logger.debug("%s uploaded the %s test files %s." % (self.__class__, sess_obj.label(), fnames))
        
        return (sess_obj.label(), fnames)
    
    def _upload_file(self, session_obj, path):
        """
        :param session_obj: the XNAT session object
        :param path: the file path
        :return: the XNAT file object label
        """
        _, fname = os.path.split(path)
        scan = re.match('series(\d{3}).nii.gz', fname).group(1)
        # The XNAT file object
        file_obj = session_obj.scan(scan).resource('NIFTI').file(fname)
        # Upload the file.
        file_obj.insert(path, experiments='xnat:MRSessionData', format='NIFTI')
        
        return fname


if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
