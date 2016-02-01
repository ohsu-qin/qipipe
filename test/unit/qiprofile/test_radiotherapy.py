from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_true,
                        assert_is_none, assert_is_not_none)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import (Treatment, Radiation, Dosage)
from qipipe.qiprofile import (xls, radiotherapy)
from ... import PROJECT
from . import BREAST_FIXTURE

SUBJECT = 2
"""The radiotherapy patient is Subject 2."""

COLLECTION = 'Breast'
"""The test collection name."""


class TestRadiotherapy(object):
    """qiprofile radiotherapy pipeline update tests."""
    
    def test_read(self):
        wb = xls.load_workbook(BREAST_FIXTURE)
        row_iter = radiotherapy.read(wb, subject_number=SUBJECT)
        rows = list(row_iter)
        assert_equal(len(rows), 1, "Radiotherapy row count for subject %s is"
                                   " incorrect: %d" %
                                   (SUBJECT, len(rows)))
        for i, row in enumerate(rows):
            for attr in ['subject_number', 'treatment_type', 'start_date',
                         'beam_type', 'amount']:
                assert_in(attr, row, "Radiotherapy row %d attribute is missing: %s" %
                                     (i + 1, attr))
            # There is no input beam type.
            assert_is_none(row.beam_type, "Radiotherapy row %d incorrectly has a"
                                          " input beam_type" % (i + 1))
            # The other values are present.
            for attr in ['subject_number', 'treatment_type', 'start_date', 'amount']:
                assert_is_not_none(row[attr], "Radiotherapy row %d attribute value is"
                                              " missing: %s" % (i + 1, attr))
    
    def test_update(self):
        # A test subject database object.
        subject = Subject(project=PROJECT, collection=COLLECTION,
                          number=SUBJECT)
        # Simulate the parsed adiotherapy rows.
        rows = [
            Bunch(subject_number=1, treatment_type='Primary', beam_type=None,
                  start_date=datetime(2014, 1, 15), duration=30, amount=20.5)
        ]
        # Update the database object.
        radiotherapy.update(subject, rows)
        # Validate the subject treatment.
        trts = subject.treatments
        assert_equal(len(trts), 1, "The treatment count is incorrect: %d" %
                                   len(trts))
        trt = trts[0]
        assert_equal(len(trt.dosages), 1, "The first treatment dosages count is"
                                          " incorrect: %d" % len(trt.dosages))
        assert_true('treatment_type' in trt, "The treatment does not have a"
                                             " treatment_type attribute")
        # Validate the dosage.
        row_ndx = 0
        for trt in trts:
            for dosage in trt.dosages:
                self._validate(trt, dosage, rows[row_ndx])
                row_ndx += 1
        # Validate the full object.
        subject.validate()
    
    def _validate(self, treatment, dosage, row):
        agent = dosage.agent
        assert_is_not_none(agent, "The dosage agent is missing")
        assert_is_not_none(agent.beam_type, "The default beam_type was not added"
                                            " to the agent")
        assert_equal(agent.beam_type, 'photon', "The dosage agent beam_type was"
                                                " not set to the default: %s" %
                                                agent.beam_type)
        for attr, expected in row.iteritems():
            if attr in Treatment._fields:
                actual = getattr(treatment, attr)
            elif attr in Radiation._fields:
                actual = getattr(dosage.agent, attr)
            elif attr in Dosage._fields:
                actual = getattr(dosage, attr)
            else:
                assert_equal(attr, 'subject_number', "The test row attribute is"
                                                     " not recognized: %s" % attr)
                continue
             # Validate the attribute values.
            assert_equal(actual, expected, "The radiotherapy %s value is incorrect:"
                                           " %s" % (attr, actual))



if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
