import os
import glob
import shutil
from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_is_none,
                        assert_is_not_none, assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import (Treatment, Drug, Radiation)
from qipipe.qiprofile import dosage
from ...helpers.logging import logger
from . import (PROJECT, BREAST_FIXTURE, BREAST_SUBJECT)


class TestDosage(object):
    """qiprofile dosage pipeline update tests."""

    def test_read(self):
        rows = list(dosage.filter(BREAST_FIXTURE, BREAST_SUBJECT))
        assert_equal(len(rows), 4, "Dosage row count for subject %s is"
                                   " incorrect: %d" %
                                   (BREAST_SUBJECT, len(rows)))
        for row_num, row in enumerate(rows):
            for attr in ['start_date', 'duration', 'agent', 'amount', 'per_weight']:
                assert_in(attr, row, "Dosage row %d attribute is missing: %s" %
                                     (row_num, attr))
            for attr in ['start_date', 'duration', 'agent', 'amount']:
                assert_is_not_none(row[attr], "Dosage row %d attribute value is"
                                             " missing: %s" % (row_num, attr))
            assert_is_none(row.per_weight, "Dosage row %d per_weight value is"
                                           "incorrect: %s" %
                                           (row_num, row.per_weight))
            assert_is_instance(row.start_date, datetime,
                               "Dosage row %d start date type is incorrect: %s" %
                               (row_num, row.start_date.__class__))
            assert_equal(row.agent, row.agent.lower(),
                               "Dosage row %d agent is not converted to lower"
                               " case: %s" % 
                               (row_num, row.agent))

    def test_update(self):
        # A test subject database object.
        trts = [
            Treatment(treatment_type='Neoadjuvant',
                      start_date=datetime(2014, 01, 15),
                      end_date=datetime(2014, 02, 14)),
            Treatment(treatment_type='Adjuvant',
                      start_date=datetime(2014, 02, 20),
                      end_date=datetime(2014, 03, 01))
        ]
        subject = Subject(project=PROJECT, treatments=trts)

        # Simulate parsed dosage rows.
        rows = [
            Bunch(start_date=datetime(2014, 01, 15), duration=30,
                  agent='docetaxel', amount=20),
            Bunch(start_date=datetime(2014, 01, 20), duration=20,
                  agent='trastuzumab', amount=10),
            Bunch(start_date=datetime(2014, 02, 01), duration=1,
                  agent='photon', amount=40),
            Bunch(start_date=datetime(2014, 02, 21), duration=30,
                  agent='pertuzumab', amount=30)
        ]

        # Update the database object.
        dosage.update(subject, rows)

        # Validate the updated subject.
        assert_equal(len(trts[0].dosages), 3, "The first treatment dosages count is"
                                              " incorrect: %d" % len(trts[0].dosages))
        assert_equal(len(trts[1].dosages), 1, "The second treatment dosages count is"
                                              " incorrect: %d" % len(trts[1].dosages))        
        
        # Validate the dosages.
        row_ndx = 0
        for trt in trts:
            for tgt in trt.dosages:
                self._validate(tgt, rows[row_ndx])
                row_ndx += 1

    def _validate(self, target, row):
        # Validate the dosage type.
        expected_type = Radiation if target.agent == 'photon' else Drug
        assert_is_instance(target, expected_type, "The dosage type is"
                                                  " incorrect: %s" %
                                                  target.__class__)
        for attr, expected in row.iteritems():
            actual = getattr(target, attr)
            # Validate the attribute values.
            assert_equal(actual, expected, "The dosage %s value is incorrect:"
                                           " %s" % (attr, actual))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
