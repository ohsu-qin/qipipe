import os
from nose.tools import (assert_equal, assert_in, assert_true)
from ...helpers.logging import logger
from qipipe.interfaces import GroupDicom
from ... import ROOT
from ...helpers.logging import logger

# The test fixture.
FIXTURE = os.path.join(ROOT, 'fixtures', 'staging', 'breast', 'BreastChemo3',
                       'Visit1', 'BC3_V1_concatenated')


class TestGroupDicom(object):

    """GroupDicom interface unit tests."""

    def test_group_dicom(self):
        logger(__name__).debug("Testing the GroupDicom interface on %s..."
                               % FIXTURE)
        grouper = GroupDicom(tag='AcquisitionNumber', in_files=FIXTURE)
        result = grouper.run()
        grp_dict = result.outputs.groups
        assert_true(not not grp_dict, "GroupDicom did not group the files")
        for volume in [1, 14]:
            assert_in(volume, grp_dict, "GroupDicom did not group the volume %d"
                                        % volume)
            assert_equal(len(grp_dict[volume]), 1, "Too many DICOM files were"
                         " grouped into volume %d: %d" %
                         (volume, len(grp_dict[volume])))
        logger(__name__).debug("GroupDicom interface test completed")


if __name__ == "__main__":
    import nose

    nose.main(defaultTest=__name__)

