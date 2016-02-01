from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_true,
                        assert_is_not_none)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import (Treatment, Drug, Dosage)
from qipipe.qiprofile import (xls, chemotherapy)
from ... import PROJECT
from . import BREAST_FIXTURE

SUBJECT = 1
"""Focus testing on the chemotherapy patient Subject 1."""

COLLECTION = 'Breast'
"""The test collection name."""

class TestChemotherapy(object):
    """qiprofile chemotherapy pipeline update tests."""
    
    def test_read(self):
        wb = xls.load_workbook(BREAST_FIXTURE)
        row_iter = chemotherapy.read(wb, subject_number=SUBJECT)
        rows = list(row_iter)
        assert_equal(len(rows), 3, "Chemotherapy row count for subject %s is"
                                   " incorrect: %d" %
                                   (SUBJECT, len(rows)))
        for i, row in enumerate(rows):
            for attr in ['subject_number', 'treatment_type', 'start_date',
                         'name', 'amount']:
                assert_in(attr, row, "Chemotherapy row %d attribute is missing: %s" %
                                     (i + 1, attr))
                assert_is_not_none(row[attr], "Chemotherapy row %d attribute value is"
                                              " missing: %s" % (i + 1, attr))
    
    def test_update(self):
        # A test subject database object.
        subject = Subject(project=PROJECT, collection=COLLECTION,
                          number=SUBJECT)
        # Simulate the parsed radiotherapy rows.
        rows = [
            Bunch(subject_number=1, treatment_type='Neoadjuvant',
                  name='pertuzumab', start_date=datetime(2014, 3, 1),
                  amount=50.0, duration=20),
            Bunch(subject_number=1, treatment_type='Neoadjuvant',
                  name='trastuzumab', start_date=datetime(2014, 3, 1),
                  amount=70.4, duration=30),
            Bunch(subject_number=1, treatment_type='Adjuvant',
                  name='pertuzumab', start_date=datetime(2014, 5, 1),
                  amount=30.2, duration=30)
        ]
        # Update the database object.
        chemotherapy.update(subject, rows)
        # Validate the subject treatment.
        trts = subject.treatments
        assert_equal(len(trts), 2, "The treatment count is incorrect: %d" %
                                   len(trts))
        # Focus on neoadjuvant.
        trt_iter = (trt for trt in trts if trt.treatment_type == 'Neoadjuvant')
        trt = next(trt_iter, None)
        assert_is_not_none(trt, "The neoadjuvant treatment was not found")
        assert_equal(len(trt.dosages), 2, "The neoadjuvant treatment dosages count is"
                                          " incorrect: %d" % len(trt.dosages))
        # Validate the dosage.
        trt_rows = [row for row in rows if row.treatment_type == trt.treatment_type]
        for i, dosage in enumerate(trt.dosages):
            self._validate_dosage(trt, dosage, trt_rows[i])
        # Validate the full object.
        subject.validate()
    
    def _validate_dosage(self, treatment, dosage, row):
        agent = dosage.agent
        assert_is_not_none(dosage.agent, "The dosage agent is missing")
        for attr, expected in row.iteritems():
            if attr in Treatment._fields:
                actual = getattr(treatment, attr)
            elif attr in Drug._fields:
                actual = getattr(dosage.agent, attr)
            elif attr in Dosage._fields:
                actual = getattr(dosage, attr)
            else:
                assert_equal(attr, 'subject_number', "The test row attribute is"
                                                     " not recognized: %s" % attr)
                continue
             # Validate the attribute values.
            assert_equal(actual, expected, "The dosage %s value is incorrect:"
                                           " %s vs %s" % (attr, actual, expected))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
