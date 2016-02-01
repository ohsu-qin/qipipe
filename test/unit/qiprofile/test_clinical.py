from datetime import datetime
from bunch import Bunch
from mongoengine import (connect, ValidationError)
from mongoengine.connection import get_db
from nose.tools import (assert_true, assert_equal, assert_in,
                        assert_is_none, assert_is_not_none,
                        assert_is_instance)
from qiprofile_rest_client.model.subject import Subject
from qipipe.qiprofile import clinical
from ...helpers.logging import logger
from ... import PROJECT
from . import (DATABASE, BREAST_FIXTURE, SARCOMA_FIXTURE)


class TestClinical(object):
    """
    Database clinical information update tests.
    
    These test cases spot check object cardinality. Detail content test
    cases are performed in the sheet test classes, e.g. ``TestDemographics``.
    """
    
    def setup(self):
        self._connection = connect(db=DATABASE)
        self._connection.drop_database(DATABASE)
    
    def tearDown(self):
      self._connection.drop_database(DATABASE)
    
    def test_breast(self):
        for n in [1, 2]:
            # Create the database object.
            subject = Subject.objects.create(project=PROJECT, collection='Breast',
                                             number=n)
            # Populate the subject from the spreadsheet.
            clinical.update(subject, BREAST_FIXTURE)
        
        # Validate the saved subjects.
        assert_equal(Subject.objects.count(), 2, "The saved subjects count is"
                                                 " incorrect: %d." %
                                                 Subject.objects.count())
        # Spot-check the first subject.
        subject = Subject.objects.get(project=PROJECT, collection='Breast',
                                      number=1)
        # All of the first subject demographics values are set.
        for attr in ['birth_date', 'gender', 'races', 'ethnicity']:
            assert_is_not_none(getattr(subject, attr),
                               "The saved Breast Subject 1 attribute %s is"
                               " missing" % attr)
        trts = subject.treatments
        assert_equal(len(trts), 2, "The saved Breast Subject 1 treatments count"
                                   " is incorrect: %d" % len(trts))
        encs = subject.encounters
        assert_equal(len(encs), 2, "The saved Breast Subject 1 encounters count"
                                   " is incorrect: %d" % len(encs))
        # Spot-check the second subject.
        subject = Subject.objects.get(project=PROJECT, collection='Breast',
                                      number=2)
        # All but the ethnicity attribute is set in the second subject
        # demographics values.
        for attr in ['birth_date', 'gender', 'races']:
            assert_is_not_none(getattr(subject, attr),
                               "The saved Breast Subject 2 attribute %s is"
                               " missing" % attr)
        trts = subject.treatments
        assert_equal(len(trts), 2, "The saved Breast Subject 2 encounters count"
                                   " is incorrect: %d" % len(trts))
        encs = subject.encounters
        assert_equal(len(encs), 1, "The saved Breast Subject 2 encounters count"
                                   " is incorrect: %d" % len(encs))
    
    def test_sarcoma(self):
        for n in [1, 2]:
            # Create the database object.
            subject = Subject.objects.create(project=PROJECT, collection='Sarcoma',
                                             number=n)
            # Populate the subject from the spreadsheet.
            clinical.update(subject, SARCOMA_FIXTURE)
        
        # Validate the saved subjects.
        assert_equal(Subject.objects.count(), 2, "The saved subjects count is"
                                                 " incorrect: %d." %
                                                 Subject.objects.count())
        # Spot-check the first subject.
        subject = Subject.objects.get(project=PROJECT, collection='Sarcoma',
                                      number=1)
        # All of the first subject demographics values are set.
        for attr in ['birth_date', 'gender', 'races']:
            assert_is_not_none(getattr(subject, attr),
                               "The saved Sarcoma Subject 1 attribute %s is"
                               " missing" % attr)
        assert_is_instance(subject.races, list, "Races is not a list")
        assert_equal(len(subject.races), 1, "Races count is incorrect: %d" %
                                            len(subject.races))
        race = subject.races[0]
        assert_equal(race, 'NHOPI', "Sarcoma Subject 1 race is incorrect:"
                                    " %s" % race)
        
        trts = subject.treatments
        assert_equal(len(trts), 2, "The saved Sarcoma Subject 1 treatments count"
                                   " is incorrect: %d" % len(trts))
        encs = subject.encounters
        assert_equal(len(encs), 2, "The saved Sarcoma Subject 1 treatments count"
                                   " is incorrect: %d" % len(encs))
        # Spot-check the second subject.
        subject = Subject.objects.get(project=PROJECT, collection='Sarcoma',
                                      number=2)
        # All but the ethnicity attribute is set in the second subject
        # demographics values.
        for attr in ['birth_date', 'gender', 'races']:
            assert_is_not_none(getattr(subject, attr),
                               "The saved Sarcoma Subject 2 attribute %s is"
                               " missing" % attr)
        trts = subject.treatments
        assert_equal(len(trts), 1, "The saved Sarcoma Subject 2 encounters count"
                                   " is incorrect: %d" % len(trts))
        encs = subject.encounters
        assert_equal(len(encs), 1, "The saved Sarcoma Subject 2 encounters count"
                                   " is incorrect: %d" % len(encs))


if __name__ == "__main__":
    import nose
    nose.main(defaultTest=__name__)
