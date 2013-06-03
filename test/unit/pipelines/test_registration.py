import sys, os, re, glob, shutil
from nose.tools import *
import nipype.pipeline.engine as pe

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe import PROJECT
from qipipe.pipelines import registration as reg
from qipipe.helpers import xnat_helper
from qipipe.helpers.xnat_helper import delete_subjects
from test.helpers.registration import VOL_CLUSTER_TEST_OPTS, ANTS_REG_TEST_OPTS

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

FIXTURES = os.path.join(ROOT, 'fixtures', 'stacks')
"""The test fixtures directory."""

REG_CONF = os.path.join(ROOT, 'conf', 'register.cfg')
"""The test registration configuration."""

RESULTS = os.path.join(ROOT, 'results', 'pipelines', 'registration')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)


class TestRegistrationPipeline:
    """
    Registration pipeline unit tests.
    
    This test exercises the registration pipeline on three series of one visit in each of the
    Breast and Sarcoma studies.
    """
    
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
        
        @param collection: the collection name
        """
        fixture = os.path.join(FIXTURES, collection.lower())
        logger.debug("Testing the registration pipeline on %s..." % fixture)
        
        # The test configuration.
        opts = dict(read_config(REG_CONF))
        # The workflow base directory.
        opts['base_dir'] = os.path.join(RESULTS, collection.lower(), 'work')
        
        # The (subject, session) => scan files dictionary.
        sess_files_dict = {}
        # The test subjects.
        subjects = set()
        with xnat_helper.connection() as xnat:
            # Seed XNAT with the test files.
            for sbj_dir in glob.glob(fixture + '/' + collection + '*'):
                _, sbj = os.path.split(sbj_dir)
                subjects.add(sbj)
                # Delete a stale test XNAT subject, if necessary.
                delete_subjects(PROJECT, sbj)
                # Populate the test XNAT subject from the test fixture.
                for sess_dir in glob.glob(sbj_dir + '/Session*'):
                    sess, in_files = self._upload_session_files(sbj, sess_dir)
                    sess_files_dict[(sbj, sess)] = in_files
            
            # Run the workflow.
            recon_specs = reg.run(*sess_files_dict.iterkeys(), **opts)
            
            # Verify the result.
            sess_recon_dict = {(sbj, sess): recon for sbj, sess, recon in recon_specs}
            for spec, in_files in sess_files_dict.iteritems():
                assert_in(spec, sess_recon_dict, "The session %s %s was not registered" % spec)
                recon = sess_recon_dict[spec]
                sbj, sess = spec
                recon_obj = xnat.get_reconstruction(PROJECT, sbj, sess, recon)
                assert_true(recon_obj.exists(),
                    "The %s %s %s reconstruction was not created" % (sbj, sess, recon))
                recon_files = recon_obj.out_resource('NIFTI').files().get()
                assert_equals(len(in_files), len(recon_files),
                    "The registered %s %s file count is incorrect - expected: %d, found: %d" %
                    (sbj, sess, len(in_files), len(recon_files)))
            
            # Clean up.
            delete_subjects(PROJECT, *subjects)
    
    def _upload_session_files(self, subject, session_dir):
        """
        Uploads the test files in the given session directory.
        
        @param subject: the test subject label
        @param session_dir: the session directory
        @return: the XNAT (session, files) labels
        """
        _, dname = os.path.split(session_dir)
        with xnat_helper.connection() as xnat:
            sess_obj = xnat.get_session(PROJECT, subject, dname)
            fnames = [self._upload_file(sess_obj, f) for f in glob.glob(session_dir + '/series*.nii.gz')]
            logger.debug("%s uploaded the %s test files %s." % (self.__class__, sess_obj.label(), fnames))
        
        return (sess_obj.label(), fnames)
    
    def _upload_file(self, session_obj, path):
        """
        @param session_obj: the XNAT session object
        @param path: the file path
        @return: the XNAT file object label
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
