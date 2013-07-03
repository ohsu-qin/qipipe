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


class XNATScanTestBase(object):
    """
    Base class for testing workflows that download XNAT scan files.
    """
    def __init__(self, fixtures, results):
        self._fixtures = fixtures
        self._results = results
    
    def setUp(self):
        shutil.rmtree(self._results, True)
    
    def tearDown(self):
        shutil.rmtree(self._results, True)
    
    def test_breast(self):
        self._test_collection('Breast')
    
    def test_sarcoma(self):
        self._test_collection('Sarcoma')
    
    def _test_collection(self, collection):
        """
        Run the workflow and verify that the registered images are
        created in XNAT.
        
        :param collection: the collection name
        """
        fixture = os.path.join(self._fixtures, collection.lower())

        # The workflow base directory.
        base_dir = os.path.join(self._results, collection.lower(), 'work')
        
        with xnat_helper.connection() as xnat:
            # Seed XNAT with the test files.
            sess_files_dict = self._seed_xnat(fixture, collection)
            # Run the workflow.
            result = self._run_workflow(xnat, fixture, *sess_files_dict.iterkeys(), base_dir=base_dir)
            # Verify the result.
            self._verify_result(xnat, sess_files_dict, result)
            # Clean up.
            subjects = {sbj for sbj, _ in sess_files_dict.iterkeys()}
            #delete_subjects(project(), *subjects)

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
        
    def _run_workflow(self, xnat, *inputs, **opts):
        """
        This method is implemented by subclasses to execute the subclass
        target workflow on the given session inputs.
        
        :param xnat: the :class:`qipipe.helpers.xnat_helpers.XNAT` connection
        :param fixture: the test fixture directory
        :param inputs: the (subject, session) tuples
        :param opts: the target workflow options
        :return: the :meth:`qipipe.pipelines.modeling.run` result
        :raise: if the class:`XNATScanTestBase` subclass does not implement
            this method
        """
        raise NotImplementedError("_run_workflow is a subclass responsibility")
    
    def _verify_result(self, xnat, inputs, result):
        """
        This method is implemented by subclasses to verify the workflow
        execution result against the given session file inputs. The session
        file inputs parameter is a {(subject, session): *files* dictionary},
        where *files* is a list consisting of the session image files found
        in the test fixture directory.
        
        :param xnat: the :class:`qipipe.helpers.xnat_helpers.XNAT` connection
        :param inputs: the session file inputs dictionary
        :param result: the workflow execution result
        :raise: if the class:`XNATScanTestBase` subclass does not implement
            this method
        """
        raise NotImplementedError("_verify_result is a subclass responsibility")
           
    def _upload_session_files(self, subject, session_dir):
        """
        Uploads the test files in the given session directory.
        
        :param subject: the test subject label
        :param session_dir: the session directory
        :return: the XNAT (session, files) labels
        """
        _, sess = os.path.split(session_dir)
        with xnat_helper.connection() as xnat:
            sess_obj = xnat.get_session(project(), subject, sess)
            fnames = [self._upload_file(sess_obj, f) for f in glob.glob(session_dir + '/series*.nii.gz')]
            logger.debug("%s uploaded the %s test files %s." %
                (self.__class__.__name__, sess_obj.label(), fnames))
        
        return (sess, fnames)
    
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
