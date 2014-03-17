"""This helper module  is a Image Profile database facade."""

import os
import re

# Set the settings environment variable before loading the models.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'admin.settings')

from qiprofile.models import (Project, Collection, Subject, Session, Modeling)
from . import xnat_helper
from .project import project
from .logging_helper import logger

SBJ_REGEX = re.compile('^(\w+?)(\d+)$')

SESS_REGEX = re.compile('^Session(\d+)$')

class QIProfile(object):
    
    """QIProfile is a facade class to access the Image Profile database."""
    
    def sync(self):
        """
        Updates the Imaging Profile database from the XNAT database.
        The subjects, sessions and modeling results in the
        :meth:`qipipe.helpers.project.project` are added, if necessary,
        to the Imaging Profile database.
        """
        prj = project()
        with xnat_helper.connection as xnat:
            criterion = "/project/%s/subjects" % prj
            for xnat_sbj in xnat.interface.select(criterion):
                prf_sbj = self.find_subject(prj, xnat_sbj.name, create=True)
                for xnat_sess in xnat_sbj.sessions():
                    prf_sess = self.find_session(prj, xnat_sbj.name,
                                                 xnat_sess.name, create=True)
                    # TODO - get image acquisition date from DICOM?
                    # TODO - add modeling
    
    def find_subject(self, project, subject, create=False):
        """
        Finds the given Imaging Profile subject. If it doesn't exists
        and the *create* flag is set, then the subject is created.
    
        :param project: the project name
        :param subject: the subject name
        :param create: flag indicating whether to create the subject
        :return: the Subject instance, or None if not found
        """
        # The collection name and subject number.
        collection, sbj_nbr_str = SBJ_REGEX.match(subject).groups()
        sbj_nbr = int(sbj_nbr_str)
        
        # Fetch the subject.
        try:
            sbj = Subject.objects.get(project__name=project,
                                collection__name=collection,
                                number=sbj_nbr)
        except Subject.DoesNotExist:
            if create:
                prj = Project.objects.get(name=project)
                coll = Collection.objects.get(name=collection)
                sbj = Subject(project=prj, collection=coll, number=sbj_nbr)
                sbj.save()
            else:
                return
        
        return sbj
    
    def find_session(self, project, subject, session, create=False):
        """
        Finds the given Imaging Profile subject. If it doesn't exists
        and the *create* flag is set, then the session is created.
    
        :param project: the project name
        :param subject: the subject name
        :param session: the session name
        :param create: flag indicating whether to create the subject
        :return: the Session instance, or None if not found
        """
        # The session number.
        sess_nbr_str = SESS_REGEX.match(session).group(1)
        sess_nbr = int(sess_nbr_str)

        # Fetch the subject.
        sbj = self.find_subject(project, subject, create)
        if not sbj:
            return
        try:
            sess = Session.objects.get(subject=sbj, number=sess_nbr)
        except Subject.DoesNotExist:
            if create:
                sess = Session(subject=subject, number=sess_nbr)
                sess.save()
            else:
                            return
        
        return sess
    
    def save_subject(self, project, subject, **opts):
        """
        Creates the Imaging Profile subject, if necessary, and updates
        the given subject fields.
    
        :param project: the project name
        :param subject: the subject name
        :param opts: the ``qiprofile.models.Subject`` non-identifier
            field name-value parameters
        :return: the Subject instance
        """
        # The qiprofile collection and subject number.
        collection, sbj_nbr_str = SBJ_REGEX.match(subject).groups()
        sbj_nbr = int(sbj_nbr_str)
    
        # Make the subject.
        prj = Project.objects.get(name=project)
        coll = Collection.objects.get(name=collection)
        sbj = Subject(project=prj, collection=coll, number=sbj_nbr)
        for attr, value in opts.iteritems():
            setattr(sbj, attr, value)

        # Save the subject.
        logger(__name__).debug("Saving %s Subject %s..." % (project, subject))
        sbj.save()
        logger(__name__).debug("Saved %s Subject %s." % (project, subject))

    def save_session(self, project, subject, session, **opts):
        """
        :param project: the project name
        :param subject: the subject name
        :param session: the session name
        :param opts: the ``qiprofile.models.Session`` non-identifier field
            name-value parameters
        :return: the Session instance
        """
        # The session number.
        sess_nbr_str = SESS_REGEX.match(session).group(1)
        sess_nbr = int(sess_nbr_str)

        # The subject.
        sbj = self.find_subject(project, subject, create=True)
        
        # Make the session.
        sess = Session(subject=sbj, number=sess_nbr, **opts)
        
        # Save the session.
        logger(__name__).debug("Saving %s Subject %s Session %s..." %
                               (project, subject, session))
        sess.save()
        logger(__name__).debug("Saved %s Subject %s Session %s." %
                               (project, subject, session))

    def save_modeling(self, project, subject, session, assessor, **opts):
        """
        :param project: the project id
        :param subject: the subject name
        :param session: the session name
        :param assessor: the XNAT assessor name
        :param opts: the ``qiprofile.models.Modeling`` non-identifier field
            name-value parameters
        :return: the Modeling instance
        """
        # The session.
        sess = self.find_session(project, subject, session, create=True)
        
        # The modeling object.
        mdl = Modeling(session=sess, name=assessor, **opts)
        mdl.save()
