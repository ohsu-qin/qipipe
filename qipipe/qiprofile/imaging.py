"""
This module updates the qiprofile database imaging information
from a XNAT scan.
"""
import csv
import tempfile
from qiutil.file import splitexts
from qiutil.ast_config import read_config
from qixnat.helpers import xnat_path
from qiprofile_rest_client.helpers import database
from qiprofile_rest_client.model.imaging import (
    Session, SessionDetail, Scan, ScanProtocol, Modeling, ModelingProtocol
)
from ..helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from ..helpers.colors import label_map_basename
from ..pipeline.staging import (SCAN_METADATA_RESOURCE, SCAN_CONF_FILE)
from ..pipeline.modeling import (MODELING_PREFIX, MODELING_CONF_FILE)
from ..pipeline.modeling import INFERRED_R1_0_OUTPUTS as OUTPUTS
from ..pipeline.roi import ROI_RESOURCE
from . import modeling


class ImagingError(Exception):
    pass


def update(subject, experiment):
    """
    Updates the imaging content for the qiprofile REST Subject
    from the XNAT experiment.

    :param subject: the target qiprofile Subject to update
    :param experiment: the XNAT experiment object
    :param opts: the :class`Updater` keyword arguments
    """
    Updater(**opts).update(subject, experiment)


class Updater(object):
    def __init__(self, **opts):
        """
        :param opts: the following keyword arguments:
        :option scan: the :attr:`scan_type`
        :option bolus_arrival_index: the :attr:`bolus_arrival_index`
        :option roi_centroid: the :attr:`roi_centroid`
        :option roi_average_intensity: the :attr:`roi_average_intensity`
        """
        self.scan_type = opts.get('scan_type')
        """
        The :attr:`qipipe.staging.image_collection.Patterns.scan` scan type,
            e.g. ``T1``, ``T2``, ``DWI`` or ``PD``.
        """

        self.bolus_arrival_index = opts.get('bolus_arrival_index')
        """
        The scan
        :meth:qipipe.pipeline.qipipeline.bolus_arrival_index_or_zero`.
        """
        
        self.roi_centroid = opts.get('roi_centroid')
        """The ROI centroid."""

        self.roi_average_intensity = opts.get('roi_average_intensity')
        """The ROI average signal intensity."""

    def update(self, subject, experiment):
        """
        Updates the imaging content for the qiprofile REST Subject
        from the XNAT experiment.
    
        :param subject: the target qiprofile Subject to update
        :param experiment: the XNAT experiment object
        """
        # The XNAT experiment must have a date.
        date = experiment.attrs.get('date')
        if not date:
            raise ImagingError( "The XNAT experiment %s is missing the"
                                " visit date" % xnat_path(experiment))
        # If there is a qiprofile session with the same date,
        # then complain.
        if any( sess.date == date for sess in subject.sessions):
            raise ImagingError(
                "a qiprofile %s %s Subject %d session with visit date %s"
                " already exists" % (subject.project, subject.collection,
                                     subject.number, date)
            )
        # Make the qiprofile session database object.
        session = _create_session(experiment)
        # Add the session to the subject encounters in date order.
        subject.add_encounter(session)
        # Save the session detail.
        session.detail.save()
        # Save the subject.
        subject.save()


    def _create_session(self, experiment):
        """
        Makes the qiprofile Session object from the XNAT scan.

        :param experiment: the XNAT experiment object
        :return: the qiprofile session object
        """
        # Make the qiprofile scans.
        scans = [_create_scan(xnat_scan)
                 for xnat_scan in experiment.scans()]

        # The modeling resources begin with 'pk_'.
        xnat_mdl_rscs = (rsc for rsc in xnat_scan.resources()
                         if rsc.label().startswith(MODELING_PREFIX))
        modelings = [_create_modeling(rsc) for rsc in xnat_mdl_rscs]

        # The session detail database object to hold the scans.
        detail = SessionDetail(scans=scans)
        # Save the detail first, since it is not embedded and we need to
        # set the detail reference to make the session.
        detail.save()

        # The XNAT experiment date is the qiprofile session date.
        date = experiment.attrs.get('date')

        # Return the new qiprofile Session object.
        return Session(date=date, modelings=modelings, detail=detail)


    def _create_scan(self, xnat_scan):
        """
        Makes the qiprofile Session object from the XNAT scan.

        :param xnat_scan: the XNAT scan object
        :return: the qiprofile scan object
        """
        # The scan number.
        number = int(xnat_scan.label())

        # The scan protocol.
        if not self.scan_type:
            raise ImagingError("The XNAT scan %s qiprofile update is missing the"
                               " scan type" % xnat_path(xnat_scan))
        scan_pcl_dict = {opt: getattr(self, opt) for opt in ScanProtocol._fields
                         if hasattr(self, opt)}

        # The corresponding qiprofile ScanProtocol.
        protocol = database.get_or_create(ScanProtocol, scan_pcl_dict)

        # There must be a bolus arrival.
        if not self.bolus_arrival_index:
            raise ImagingError("The XNAT scan %s qiprofile update is missing the"
                               " bolus arrival" % xnat_path(xnat_scan))
        bolus_arv_ndx = self.bolus_arrival_index

        # The ROI resource.
        roi_rsc = xnat_scan.resource(ROI_RESOURCE)
        if roi_rsc.exists():
            roi_files = set(roi_rsc.files().get())
            # Group by lesion.
            masks = (f for f in roi_files if not '_color' in f)
            for mask in masks:
                label_map_fname = label_map_basename(mask)
                if label_map_fname in roi_files:
                    # TODO - get the common color table option.
                    label_map = LabelMap(filename=label_map_fname)
                
    
        # There might be a registration.
    

        # Return the new qiprofile Scan object.
        return Scan(number=number, protocol=protocol,
                    bolus_arrival_index=bolus_arv_ndx)


    def _create_region(self, mask):
        """
        :param mask: the ROI mask XNAT file name
        :param opts: the following keyword arguments:
        :option centroid: the region centroid
        :option average_intensity: the average signal in the region
        :option label_map: the ROI mask XNAT file name
        """
        label_map_fname = opts.get('label_map')
        if self.label_map_fname:
            # The color look-up table.
            lut = opts.get('color_table')
            lobel_map_opts = dict(color_table: lut) if lut else {}
            # The label map embedded object.
            label_map = LabelMap(filename=label_map_fname, **label_map_opts)
        region_opts = {opt: opts[opt] for opt in Region._fields if opt in opts}

        return Region(mask=mask)


    def _create_registration(self, xnat_resource):
        """
        Makes the qiprofile Registration object from the XNAT resource.

        :param xnat_resource: the XNAT registration resource object
        :return: the qiprofile registration object
        """
        # TODO
        pass


    def _create_modeling(self, xnat_resource):
        """
        Creates the qiprofile Modeling object from the given XNAT
        resource object.

        :param xnat_resource: the modeling source XNAT resource object
        """
        # The XNAT modeling files.
        xnat_files = xnat_resource.files()

        # The modeling configuration.
        cfg_file_finder = (xnat_file for xnat_file in xnat_files
                           if xnat_file.label() == MODELING_CONF_FILE)
        xnat_cfg_file = next(cfg_file_finder, None)
        if not xnat_cfg_file:
            raise ImagingError("The XNAT modeling resource %s does not contain"
                               " the modeling profile file %s" %
                               (xnat_path(xnat_resource), MODELING_CONF_FILE))
        cfg_file = xnat_cfg_file.get()
        cfg = dict(read_config(cfg_file))

        # Pull out the source.
        source_topic = cfg.pop('Source', None)
        if not source_topic:
            raise ImagingError("The XNAT modeling configuration %s is missing"
                               " the Source topic" % xnat_path(cfg_file))
        source_rsc = source_topic.get('resource')
        if not source_rsc:
            raise ImagingError("The XNAT modeling configuration %s Source topic"
                               " is missing the resource option" %
                               xnat_path(cfg_file))

        # The corresponding qiprofile ModelingProtocol.
        key = dict(configuration=cfg)
        protocol = database.get_or_create(ModelingProtocol, key)

        # The qiprofile modeling output files.
        xnat_file_labels = {xnat_file.label() for xnat_file in xnat_files}
        result = {}
        for output in OUTPUTS:
            fname = output + '.nii.gz'
            if fname in xnat_file_labels:
                # TODO - add the param result average and label map to the
                # pipeline and here.
                param_result = Modeling.ParameterResult(filename=fname)
                result[output] = param_result

        # The modeling object.
        mdl = Modeling(protocol=protocol, source=source,
                       resource=xnat_resource.label(), result=result)
        # TODO - save it.
