import os
import shutil
from nose.tools import (assert_true, assert_equal, assert_not_equal, assert_is_not_none)
from qiutil.ast_config import read_config
from qipipe.helpers import metadata
from qipipe.helpers.constants import CONF_DIR
from qipipe.pipeline.registration import FNIRT_CONF_SECTIONS
from ... import ROOT

RESULTS = os.path.join(ROOT, 'results', 'helpers')


class TestMetadata(object):

    def setup(self):
        shutil.rmtree(RESULTS, True)

    def tearDown(self):
        shutil.rmtree(RESULTS, True)

    def test_create_profile(self):
        in_file = os.path.join(CONF_DIR, 'reg_test.cfg')
        in_cfg = dict(read_config(in_file))
        dest = os.path.join(RESULTS, 'reg_test.cfg')
        self.profile = metadata.create_profile(in_cfg, FNIRT_CONF_SECTIONS,
                                               dest=dest)
        assert_true(os.path.exists(dest),
                    "The profile was not created")
        prf_cfg = dict(read_config(dest))
        section_map = {s: s.split('.')[-1] for s in FNIRT_CONF_SECTIONS}
        prf_sections = prf_cfg.keys()
        assert_equal(set(prf_sections), set(section_map.values()),
                     "The profile sections are incorrect: %s" % prf_sections)
        for in_section, prf_section in section_map.iteritems():
            actual = prf_cfg[prf_section]
            expected = {k: v for k, v in in_cfg[in_section].iteritems()
                        if k not in metadata.EXCLUDED_OPTS}
            assert_equal(actual, expected,
                         "The profile section %s items are incorrect: %s" %
                         (prf_section, actual))



if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)
