import os
from nose.tools import (assert_equal, assert_in, assert_true)
from qipipe.helpers.logging_helper import logger
from qipipe.interfaces import GroupDicom
from test import ROOT

# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'breast', 'BreastChemo3', 'Visit1')

class TestGroupDicom(object):
    """GroupDicom interface unit tests."""
    
    def test_link_dicom(self):
        logger(__name__).debug("Testing the GroupDicom interface on %s..." % FIXTURE)
        grouper = GroupDicom(in_files=FIXTURE)
        result = grouper.run()
        ser_dict = result.outputs.series_files_dict
        assert_true(not not ser_dict, "GroupDicom did not group the files")
        for series in [7, 33]:
            assert_in(series, ser_dict, "GroupDicom did not group the"
                " series %d" % series)
            assert_equal(len(ser_dict[series]), 1, "Too many DICOM files were"
                " grouped in series %d: %d" % (series, len(ser_dict[series])))
        logger(__name__).debug("GroupDicom interface test completed")

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
