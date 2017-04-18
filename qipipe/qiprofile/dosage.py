"""
This module updates the qiprofile database Subject drug dosage
information from a Chemotherapy Excel worksheet.
"""
import datetime
from qirest_client.model.clinical import (Treatment, Dosage)
from .xls import Worksheet
from . import parse


class DosageError(Exception):
    pass


class DosageWorksheet(Worksheet):
    """The dosage worksheet facade."""
    
    def __init__(self, workbook, sheet, agent_class, **opts):
        """
        :param workbook: the :class:`qipipe.qiprofile.xls.Workbook` object
        :param sheet: the sheet name
        :param agent_class: the agent class
        :param opts: the additional :class:`qipipe.qiprofile.xls.Worksheet`
            initializer options
        """
        # Delegate to the superclass with the Treatment, Dosage and agent
        # classes.
        super(DosageWorksheet, self).__init__(
            workbook, sheet, Treatment, Dosage, agent_class, **opts
        )


class DosageUpdate(object):
    """The dosage update abstract class."""
    
    DEFAULTS = dict(duration=1)
    """The default duration is 1 day."""
    
    def __init__(self, subject, agent_class, **defaults):
        """
        :param subject: the ``Subject`` Mongo Engine database object
            to update
        :param agent_class: the dosage agent class
        :param defaults: the {attribute: value} row defaults
        """
        self._subject = subject
        for attr, val in DosageUpdate.DEFAULTS.iteritems():
            if attr not in defaults:
                defaults[attr] = val
        self._agent_class = agent_class
        self._defaults = defaults
    
    def update(self, rows):
        """
        Updates the subject data object from the given dosage XLS rows.
        
        :param rows: the input dosage
            :meth:`qipipe.qiprofile.xls.Worksheet.read` rows list
        """
        for row in rows:
            self._update_for(row)
    
    def _update_for(self, row):
        """
        :param row: the input dosage
            :meth:`qipipe.qiprofile.xls.Worksheet.read` row
        """
        # Apply the defaults.
        for attr, val in self._defaults.iteritems():
            if getattr(row, attr) == None:
                setattr(row, attr, val)
        
        # The treatment object for the input treatment type.
        trt = self._treatment_for(row.treatment_type)
        # Extend the treatment span, if necessary.
        if not trt.start_date or trt.start_date > row.start_date:
            trt.start_date = row.start_date
        delta = datetime.timedelta(row.duration)
        end_date = row.start_date + delta
        if not trt.end_date or trt.end_date < end_date:
            trt.end_date = end_date
        
        # If there is no amount, then we can only store the
        # treatment without dosages.
        if not row.amount:
            return
        # Find or make the target dosage object.
        dosage = self._dosage_for(trt, row)
        # Collect the update attributes.
        attrs = (attr for attr in Dosage._fields if attr in row)
        # Update the target dosage database object.
        for attr in attrs:
            setattr(dosage, attr, row[attr])
    
    def _treatment_for(self, treatment_type):
        """
        :param treatment_type: the treatment type, e.g. ``adjuvant``
        :return: the existing or new treatment object
        """
        # Find the matching treatment, if it exists.
        trt_iter = (trt for trt in self._subject.treatments
                    if trt.treatment_type == treatment_type)
        target = next(trt_iter, None)
        if not target:
            target = Treatment(treatment_type=treatment_type)
            self._subject.treatments.append(target)
        
        # Return the target treatment.
        return target
    
    def _dosage_for(self, treatment, row):
        """
        :param treatment: the target treatment
        :param row: the input row
        :return: the dosage database object which matches the start
            date and agent attribute, or a new dosage database
            object if there is no match
        :raise DosageError: if the key does not include the *start_date*
        """
        # Find the matching treatment dosage by start date and agent
        # content.
        dosage_iter = (dosage for dosage in treatment.dosages
                       if self._is_dosage_match(dosage, row))
        target = next(dosage_iter, None)
        # If no match, then make a new dosage database object.
        if not target:
            # The dosages are in date order. Insert the new dosage
            # directly before other dosages with a later date, or
            # the end of the list is there is no later dosage.
            index_iter = (i for i, dosage in enumerate(treatment.dosages)
                          if dosage.start_date > row.start_date)
            # The dosage insertion point.
            index = next(index_iter, len(treatment.dosages))
            # The new agent {attribute: value} content.
            agent_content = {attr: getattr(row, attr)
                             for attr in self._agent_class._fields
                             if hasattr(row, attr)}
            # The new agent object.
            agent = self._agent_class(**agent_content)
            # The new dosage object.
            target = Dosage(agent=agent, start_date=row.start_date)
            # Add the new dosage.
            treatment.dosages.insert(index, target)
        
        return target
    
    def _is_dosage_match(self, dosage, row):
        return (dosage.start_date == row.start_date
                and self._is_agent_match(dosage.agent, row))
    
    def _is_agent_match(self, agent, row):
        return all(getattr(agent, attr) == getattr(row, attr)
                   for attr in agent.__class__._fields
                   if hasattr(row, attr))
        