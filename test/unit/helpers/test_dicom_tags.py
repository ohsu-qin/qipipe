import unittest
import os
from qipipe.helpers import dicom_tags as tags

# The test fixture.
FIXTURE = os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'brain')

# The patient-study-series-image hierarchy for the 20 images.
HIERARCHY = [['123565', '8811', '2', str(i)] for i in range(1, 21)]

class TestDicomTags(unittest.TestCase):
    """dicom_tags unit tests."""
    
    def test_read_image_hierarchy(self):
        h = tags.read_image_hierarchy(FIXTURE)
        self.assertEqual(HIERARCHY, list(h))

if __name__ == "__main__":
    unittest.main()
