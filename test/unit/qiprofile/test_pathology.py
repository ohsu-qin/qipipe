import os
import glob
import shutil
from bunch import Bunch
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_is_none,
                        assert_is_not_none, assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qiprofile_rest_client.model.clinical import (Biopsy, Treatment)
from qipipe.qiprofile import pathology
from ...helpers.logging import logger
from . import (PROJECT, BREAST_FIXTURE, BREAST_SUBJECT, SARCOMA_FIXTURE,
               SARCOMA_SUBJECT)


class TestPathology(object):
    """qiprofile pathology pipeline update tests."""

    def test_breast_read(self):
        rows = list(pathology.filter(BREAST_FIXTURE, BREAST_SUBJECT, 'Breast'))
        assert_equal(len(rows), 2, "Pathology row count for subject %s is"
                                   " incorrect: %d" %
                                   (BREAST_SUBJECT, len(rows)))
        for row_num, row in enumerate(rows):
            for attr in ['start_date', 'duration', 'agent', 'amount', 'per_weight']:
                assert_in(attr, row, "Pathology row %d attribute is missing: %s" %
                                     (row_num, attr))
            for attr in ['start_date', 'duration', 'agent', 'amount']:
                assert_is_not_none(row[attr], "Pathology row %d attribute value is"
                                             " missing: %s" % (row_num, attr))
            assert_is_none(row.per_weight, "Pathology row %d per_weight value is"
                                           "incorrect: %s" %
                                           (row_num, row.per_weight))
            assert_is_instance(row.start_date, datetime,
                               "Pathology row %d start date type is incorrect: %s" %
                               (row_num, row.start_date.__class__))
            assert_equal(row.agent, row.agent.lower(),
                               "Pathology row %d agent is not converted to lower"
                               " case: %s" % 
                               (row_num, row.agent))

    def test_breast_update(self):
        # A test subject database object.
        trts = [
            Treatment(treatment_type='Neoadjuvant',
                      start_date=datetime(2014, 01, 15),
                      end_date=datetime(2014, 02, 14)),
            Treatment(treatment_type='Adjuvant',
                      start_date=datetime(2014, 02, 20),
                      end_date=datetime(2014, 03, 01))
        ]
        subject = Subject(project=PROJECT, collection='Breast', number=1,
                          treatments=trts)

        # Simulate parsed pathology rows.
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
        pathology.update(subject, rows)

        # Validate the updated subject.
        assert_equal(len(trts[0].pathologys), 3, "The first treatment pathologys count is"
                                              " incorrect: %d" % len(trts[0].pathologys))
        assert_equal(len(trts[1].pathologys), 1, "The second treatment pathologys count is"
                                              " incorrect: %d" % len(trts[1].pathologys))        
        
        # Validate the pathologys.
        row_ndx = 0
        for trt in trts:
            for tgt in trt.pathologys:
                self._validate(tgt, rows[row_ndx])
                row_ndx += 1

    def test_sarcoma_read(self):
        rows = list(pathology.filter(SARCOMA_FIXTURE, SARCOMA_SUBJECT, 'Sarcoma'))
        assert_equal(len(rows), 2, "Pathology row count for subject %s is"
                                   " incorrect: %d" %
                                   (SARCOMA_SUBJECT, len(rows)))
        for row_num, row in enumerate(rows):
            for attr in ['start_date', 'duration', 'agent', 'amount', 'per_weight']:
                assert_in(attr, row, "Pathology row %d attribute is missing: %s" %
                                     (row_num, attr))
            for attr in ['start_date', 'duration', 'agent', 'amount']:
                assert_is_not_none(row[attr], "Pathology row %d attribute value is"
                                             " missing: %s" % (row_num, attr))
            assert_is_none(row.per_weight, "Pathology row %d per_weight value is"
                                           "incorrect: %s" %
                                           (row_num, row.per_weight))
            assert_is_instance(row.start_date, datetime,
                               "Pathology row %d start date type is incorrect: %s" %
                               (row_num, row.start_date.__class__))
            assert_equal(row.agent, row.agent.lower(),
                               "Pathology row %d agent is not converted to lower"
                               " case: %s" % 
                               (row_num, row.agent))

    def test_sarcoma_update(self):
        # A test subject database object.
        trts = [
            Treatment(treatment_type='Neoadjuvant',
                      start_date=datetime(2014, 01, 15),
                      end_date=datetime(2014, 02, 14)),
            Treatment(treatment_type='Adjuvant',
                      start_date=datetime(2014, 02, 20),
                      end_date=datetime(2014, 03, 01))
        ]
        subject = Subject(project=PROJECT, collection='Sarcoma', number=1,
                          treatments=trts)

        # Simulate parsed pathology rows.
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
        pathology.update(subject, rows)

        # Validate the updated subject.
        assert_equal(len(trts[0].pathologys), 3, "The first treatment pathologys count is"
                                              " incorrect: %d" % len(trts[0].pathologys))
        assert_equal(len(trts[1].pathologys), 1, "The second treatment pathologys count is"
                                              " incorrect: %d" % len(trts[1].pathologys))        
        
        # Validate the pathologys.
        row_ndx = 0
        for trt in trts:
            for tgt in trt.pathologys:
                self._validate(tgt, rows[row_ndx])
                row_ndx += 1

    def _validate(self, target, row):
        # Validate the pathology type.
        expected_type = Radiation if target.agent == 'photon' else Drug
        assert_is_instance(target, expected_type, "The pathology type is"
                                                  " incorrect: %s" %
                                                  target.__class__)
        for attr, expected in row.iteritems():
            actual = getattr(target, attr)
            # Validate the attribute values.
            assert_equal(actual, expected, "The pathology %s value is incorrect:"
                                           " %s" % (attr, actual))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
