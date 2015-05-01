import os
import glob
from nose.tools import (assert_equal, assert_not_equal, assert_is_not_none)
import qixnat
from qipipe.staging.iterator import iter_stage
from ... import (ROOT, PROJECT)
from ...helpers.logging import logger
from ...helpers.staging import subject_sources

FIXTURES = os.path.join(ROOT, 'fixtures', 'staging')
"""The test fixtures parent directory."""


class TestStagingIterator(object):
    """iter_stage unit tests."""
    def test_breast(self):
        discovered = self._test_collection('Breast')
        # The first visit has a T1, T2 and DWI scan with numbers
        # 1, 2 and 4, resp. Visit 2 has only a T1 scan.
        expected_scans = {1: set([1, 2, 4]), 2: set([1])}
        for visit in [1, 2]:
            session = "Session0%d" % visit
            scans = {scan_input.scan for scan_input in discovered
                     if scan_input.session == session}
            assert_equal(scans, expected_scans[visit],
                         "Discovered Breast003 %s scans are incorrect: %s" %
                         (session, scans))
        for scan_input in discovered:
            # Validate the DICOM inputs.
            if scan_input.session == "Session01":
                # T1 has two volumes, T2 and DWI have one volume.
                expected_vol_cnt = 2 if scan_input.scan == 1 else 1
                expected_dcm_cnt = 2
            else:
                # Visit 2 T1 has two files in one volume.
                expected_vol_cnt = 1
                expected_dcm_cnt = 2
            dicom = scan_input.iterators.dicom
            volumes = dicom.keys()
            concat = lambda x,y: x + y
            dcm_files = reduce(concat, dicom.values())
            assert_equal(len(volumes), expected_vol_cnt,
                         "%s %s scan %d input volume count is"
                         " incorrect: %d" %
                         (scan_input.subject, scan_input.session,
                          scan_input.scan, len(volumes)))
            assert_equal(len(dcm_files), expected_dcm_cnt,
                         "%s %s scan %d input DICOM file count is"
                         " incorrect: %d" %
                         (scan_input.subject, scan_input.session,
                          scan_input.scan, len(dcm_files)))
            
            # Validate the ROI inputs.
            rois = scan_input.iterators.roi
            # Only T1 has ROIs.
            if scan_input.scan == 1:
                if scan_input.session == "Session01":
                    # The first visit has two lesions.
                    expected_lesions = set([1, 2])
                    expected_slices = set([12, 13])
                else:
                    # The second visit has one lesion.
                    expected_lesions = set([1])
                    expected_slices = set([12, 13])
            else:
                expected_lesions = set([])
                expected_slices = set([])
            expected_roi_cnt = len(expected_lesions) * len(expected_slices)
            assert_equal(len(rois), expected_roi_cnt,
                         "%s %s scan %d input ROI file count is"
                         " incorrect: %d" %
                         (scan_input.subject, scan_input.session,
                          scan_input.scan, len(rois)))
            lesions = set((roi.lesion for roi in rois))
            assert_equal(lesions, expected_lesions,
                         "%s %s scan %d input ROI lesions are"
                         " incorrect: %s" %
                         (scan_input.subject, scan_input.session,
                          scan_input.scan, lesions))
            slices = set((roi.slice for roi in rois))
            assert_equal(slices, expected_slices,
                         "%s %s scan %d input ROI slices are"
                         " incorrect: %s" %
                         (scan_input.subject, scan_input.session,
                          scan_input.scan, slices))


    def test_sarcoma(self):
        self._test_collection('Sarcoma')

    def _test_collection(self, collection):
        """
        Iterate on the given collection fixture subdirectory.
        
        :param collection: the AIRC collection name
        """
        fixture = os.path.join(FIXTURES, collection.lower())
        # The test {subject: directory} dictionary.
        sbj_dir_dict = subject_sources(collection, fixture)
        # The test subjects.
        subjects = set(sbj_dir_dict.keys())
        # The test source directories.
        inputs = sbj_dir_dict.values()
        # Delete the existing test subjects, since staging only detects
        # new visits.
        with qixnat.connect() as xnat:
            xnat.delete_subjects(PROJECT, *subjects)
        
        # Iterate over the scans.
        discovered = list(iter_stage(PROJECT, collection, *inputs))
        assert_not_equal(len(discovered), 0, 'No scan images were discovered')
        discovered_sbjs = set((scan_input.subject for scan_input in discovered))
        assert_equal(discovered_sbjs, subjects, "Subjects are incorrect: %s" %
                                                discovered_sbjs)
        for scan_input in discovered:
            assert_is_not_none(scan_input.session,
                               "%s scan input session is missing" %
                               scan_input.subject)
            assert_is_not_none(scan_input.scan,
                               "%s %s scan input scan number is missing" %
                               (scan_input.subject, scan_input.session))
            assert_is_not_none(scan_input.iterators,
                               "%s %s scan %d input iterators is missing" %
                               (scan_input.subject, scan_input.session, scan_input.scan)) 
            dicom = scan_input.iterators.dicom
            assert_is_not_none(dicom,
                               "%s %s scan %d input DICOM iterator is missing" %
                               (scan_input.subject, scan_input.session, scan_input.scan))
        
        # Return the iterated tuples for further testing.
        return discovered    

if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)

