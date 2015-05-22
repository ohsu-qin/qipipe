import os
import glob
import shutil
from datetime import datetime
from nose.tools import (assert_equal, assert_in, assert_is_none,
                        assert_is_not_none, assert_is_instance)
from qipipe.qiprofile import dosage
from ...helpers.logging import logger
from . import (BREAST_FIXTURES, BREAST_SUBJECT, SESSION)


class TestDosage(object):
    """
    Dosage update tests.
    """

    def test_read(self):
        rows = list(dosage.filter(BREAST_FIXTURES.dosage, BREAST_SUBJECT))
        assert_equal(len(rows), 2, "Dosage row count for subject %s is"
                                   " incorrect: %d" %
                                   (BREAST_SUBJECT, len(rows)))
        for row_num, row in enumerate(rows):
            for attr in ['subject', 'start_date', 'duration', 'agent', 'amount',
                         'per_weight']:
                assert_in(attr, row, "Dosage row %d attribute is missing: %s" %
                                     (row_num, attr))
            for attr in ['subject', 'start_date', 'duration', 'agent', 'amount']:
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


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
