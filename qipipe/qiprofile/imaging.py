"""
This module updates the qiprofile database imaging information
from a XNAT scan.
"""
import csv
import tempfile
from qiutil.file import splitexts
from qiutil.ast_config import read_config
from qixnat.helpers import (xnat_path, xnat_name)
from qiprofile_rest_client.helpers import database
from qiprofile_rest_client.model.imaging import (
    Session, SessionDetail, Scan, ScanProtocol, Modeling, ModelingProtocol
)
from ..helpers.constants import (SUBJECT_FMT, SESSION_FMT)
from ..helpers.colors import label_map_basename
from ..pipeline.staging import (SCAN_METADATA_RESOURCE, SCAN_CONF_FILE)
from ..pipeline.registration import (REG_PREFIX, REG_CONF_FILE)
from ..pipeline.modeling import (MODELING_PREFIX, MODELING_CONF_FILE)
from ..pipeline.modeling import INFERRED_R1_0_OUTPUTS as OUTPUTS
from ..pipeline.roi import ROI_RESOURCE
from . import modeling


class ImagingUpdateError(Exception):
    """``qiprofile`` imaging update error."""
    pass


def update(subject, experiment, **opts):
    """
    Updates the imaging content for the qiprofile REST Subject
    from the XNAT experiment.

    :param collection: the :attr:`qipipe.staging.image_collection.name``
    :param subject: the target qiprofile Subject to update
    :param experiment: the XNAT experiment object
    :param opts: the :class`Updater` keyword arguments
    """
    Updater(subject, **opts).update(experiment)


class Updater(object):
    def __init__(self, subject, **opts):
        """
        :param subject: the XNAT :attr:`subject`
        :param opts: the following keyword arguments:
        :option bolus_arrival_index: the :attr:`bolus_arrival_index`
        :option roi_centroid: the :attr:`roi_centroid`
        :option roi_average_intensity: the :attr:`roi_average_intensity`
        """
        self.subject = subject
        """The target qiprofile Subject to update."""

        self.bolus_arrival_index = opts.get('bolus_arrival_index')
        """
        The scan
        :meth:qipipe.pipeline.qipipeline.bolus_arrival_index_or_zero`.
        """
        
        self.roi_centroid = opts.get('roi_centroid')
        """The ROI centroid."""

        self.roi_average_intensity = opts.get('roi_average_intensity')
        """The ROI average signal intensity."""

    def update(self, experiment):
        """
        Updates the imaging content for the qiprofile REST Subject
        from the XNAT experiment.
    
        :param experiment: the XNAT experiment object
        :raise ImagingUpdateError: if the XNAT experiment does not have
            a visit date
        :raise ImagingUpdateError: if the ``qiprofile`` REST database session
            with the same visit date already exists
        """
        # The XNAT experiment must have a date.
        date = experiment.attrs.get('date')
        if not date:
            raise ImagingUpdateError( "The XNAT experiment %s is missing the"
                                " visit date" % xnat_path(experiment))
        # If there is a qiprofile session with the same date,
        # then complain.
        if any( sess.date == date for sess in self.subject.sessions):
            raise ImagingUpdateError(
                "a qiprofile %s %s Subject %d session with visit date %s"
                " already exists" % (self.subject.project,
                                     self.subject.collection,
                                     self.subject.number, date)
            )
        # Make the qiprofile Session object.
        session = self._create_session(experiment)
        # Add the session to the subject encounters in date order.
        self.subject.add_encounter(session)
        # Save the session detail.
        session.detail.save()
        # Save the subject.
        self.subject.save()

    def _create_session(self, experiment):
        """
        Makes the qiprofile Session object from the given XNAT experiment.

        :param experiment: the XNAT experiment object
        :return: the qiprofile Session object
        """
        # Make the qiprofile scans.
        scans = [self._create_scan(xnat_scan)
                 for xnat_scan in experiment.scans()]

        # The modeling resources begin with 'pk_'.
        xnat_mdl_rscs = (rsc for rsc in xnat_scan.resources()
                         if rsc.label().startswith(MODELING_PREFIX))
        modelings = [self._create_modeling(rsc) for rsc in xnat_mdl_rscs]

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
        # The image collection.
        collection = image_collection.with_name(self.subject.collection)
        # Determine the scan type from the collection and scan number.
        scan_type = collection.scan.get(number)
        if not scan_type:
            raise ImagingUpdateError(
                "The %s XNAT scan number %s is not recognized" %
                (self.subject.collection, number)
            )
        # Collect the scan protocol fields into a database key.
        key = {opt: getattr(self, opt) for opt in ScanProtocol._fields
               if hasattr(self, opt)}
        # Add the scan type.
        key['scan_type'] = scan_type
        # The corresponding qiprofile ScanProtocol.
        protocol = database.get_or_create(ScanProtocol, key)

        # There must be a bolus arrival.
        if not self.bolus_arrival_index:
            raise ImagingUpdateError("The XNAT scan %s qiprofile update is"
                                    " missing the bolus arrival" %
                                    xnat_path(xnat_scan))
        bolus_arv_ndx = self.bolus_arrival_index

        # The ROIs.
        rois = self._create_rois(xnat_scan)

        # The XNAT registration resources begin with reg_.
        registrations = [self._create_registration(rsc)
                         for rsc in xnat_scan.resources()
                         if rsc.label().startswith(REG_PREFIX)]

        # Return the new qiprofile Scan object.
        return Scan(number=number, protocol=protocol,
                    bolus_arrival_index=bolus_arv_ndx,
                    rois = rois, registrations=registrations)

    def _create_rois(self, xnat_scan):
        """
        :param xnat_scan: the XNAT scan object
        :return: the qiprofile Regions list for each lesion, indexed
            by lesion number
        """
        # The ROI resource.
        rsc = xnat_scan.resource(ROI_RESOURCE)
        # The [(mask, label map)] list.
        rois = []
        if rsc.exists():
            # The file object label is the file base name.
            fnames = set(rsc.files().get())
            # The mask files do not have _color in the prefix.
            # Sort the mask files by the lesion prefix.
            masks = sorted(f for f in fnames if not '_color' in f)
            # Make the (mask, label map) tuples.
            for mask in masks:
                roi = Region(mask=mask)
                label_map_fname = label_map_basename(mask)
                if label_map_fname in fnames:
                    # TODO - set the label map color_table property
                    # to the common color table option.
                    label_map = LabelMap(filename=label_map_fname)
                else:
                    label_map = None
                rois.append((mask, label_map))

        return rois
        
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
            lobel_map_opts = dict(color_table=lut) if lut else {}
            # The label map embedded object.
            label_map = LabelMap(filename=label_map_fname, **label_map_opts)
        region_opts = {opt: opts[opt] for opt in Region._fields if opt in opts}

        return Region(mask=mask)

    def _create_registration(self, resource):
        """
        Makes the qiprofile Registration object from the XNAT resource.

        :param resource: the XNAT registration resource object
        :return: the qiprofile Registration object
        """
        # TODO
        pass

    def _create_modeling(self, resource):
        """
        Creates the qiprofile Modeling object from the given XNAT
        resource object.

        :param resource: the modeling source XNAT resource object
        :return: the qiprofile Modeling object
        """
        # The XNAT modeling files.
        xnat_files = resource.files()

        # The modeling configuration.
        cfg_file_finder = (xnat_file for xnat_file in xnat_files
                           if xnat_file.label() == MODELING_CONF_FILE)
        xnat_cfg_file = next(cfg_file_finder, None)
        if not xnat_cfg_file:
            raise ImagingUpdateError("The XNAT modeling resource %s does"
                               " not contain the modeling profile file %s" %
                               (xnat_path(resource), MODELING_CONF_FILE))
        cfg_file = xnat_cfg_file.get()
        cfg = dict(read_config(cfg_file))

        # Pull out the source.
        source_topic = cfg.pop('Source', None)
        if not source_topic:
            raise ImagingUpdateError("The XNAT modeling configuration %s is"
                               " missing the Source topic" % xnat_path(cfg_file))
        source_rsc = source_topic.get('resource')
        if not source_rsc:
            raise ImagingUpdateError("The XNAT modeling configuration %s Source"
                               " topic is missing the resource option" %
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

        # Return the new qiprofile Modeling object.
        return Modeling(protocol=protocol, source=source,
                        resource=resource.label(), result=result)
