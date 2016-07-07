"""
This module updates the qiprofile database imaging information
from a XNAT scan.
"""
import csv
import tempfile
from qiutil.file import splitexts
from qiutil.collections import concat
from qiutil.ast_config import read_config
from qixnat.helpers import (xnat_path, xnat_name, parse_xnat_date)
from qirest_client.helpers import database
from qirest_client.model.imaging import (
    Session, SessionDetail, Scan, Volume, ScanProtocol, Registration,
    RegistrationProtocol, Modeling, ModelingProtocol
)
from ..staging import image_collection
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
        :option roi_centroids: the :attr:`roi_centroids`
        :option roi_average_intensities: the :attr:`roi_average_intensities`
        """
        self.subject = subject
        """The target qiprofile Subject to update."""
        
        self.bolus_arrival_index = opts.get('bolus_arrival_index')
        """
        The scan
        :meth:qipipe.pipeline.qipipeline.bolus_arrival_index_or_zero`.
        """
        
        self.roi_centroids = opts.get('roi_centroids')
        """The ROI centroids list in lesion number order."""
        
        self.roi_average_intensities = opts.get('roi_average_intensities')
        """The ROI average signal intensities list in lesion number order."""
    
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
        # The XNAT experiment must have a date. The XNAT date is
        # unfortunately a string, which must be converted to a
        # datetime.
        date_s = experiment.attrs.get('date')
        if not date_s:
            raise ImagingUpdateError( "The XNAT experiment %s is missing the"
                                " visit date" % xnat_path(experiment))
        date = parse_xnat_date(date_s)
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
                         if xnat_name(rsc).startswith(MODELING_PREFIX))
        modelings = [self._create_modeling(rsc, scans) for rsc in xnat_mdl_rscs]
        
        # The session detail database object to hold the scans.
        detail = SessionDetail(scans=scans)
        # Save the detail first, since it is not embedded and we need to
        # set the detail reference to make the session.
        detail.save()
        
        # The qiprofile session date is the XNAT experiment date.
        # See the comment in the update method in regards to the
        # type conversion.
        date_s = experiment.attrs.get('date')
        date = parse_xnat_date(date_s)
        
        # Return the new qiprofile Session object.
        return Session(date=date, modelings=modelings, detail=detail)
    
    def _create_scan(self, xnat_scan):
        """
        Makes the qiprofile Session object from the XNAT scan.
        
        :param xnat_scan: the XNAT scan object
        :return: the qiprofile scan object
        """
        # The scan number.
        number = int(xnat_name(xnat_scan))
        # The image collection.
        collection = image_collection.with_name(self.subject.collection)
        # Determine the scan type from the collection and scan number.
        scan_type = collection.scan_types.get(number)
        if not scan_type:
            raise ImagingUpdateError(
                "The %s XNAT scan number %s is not recognized" %
                (self.subject.collection, number)
            )
        # The scan protocol database key.
        pcl_key = dict(technique=scan_type)
        # The corresponding qiprofile ScanProtocol.
        protocol = database.get_or_create(ScanProtocol, pcl_key)
        
        # There must be a time series.
        rsc = xnat_scan.resource('scan_ts')
        if not rsc.exists():
            raise ImagingUpdateError("The XNAT scan %s does not have a time series"
                                     " resource" % xnat_path(xnat_scan))
        time_series_file = next(f for f in rsc.files())
        time_series = TimeSeries(name=xnat_name(time_series_file)
        
        # There must be a bolus arrival.
        if not self.bolus_arrival_index:
            raise ImagingUpdateError("The XNAT scan %s qiprofile update is"
                                    " missing the bolus arrival" %
                                    xnat_path(xnat_scan))
        
        # The ROIs.
        rois = self._create_rois(xnat_scan)
        
        # The XNAT registration resources begin with reg_.
        registrations = [self._create_registration(rsc)
                         for rsc in xnat_scan.resources()
                         if xnat_name(rsc).startswith(REG_PREFIX)]
        
        # Return the new qiprofile Scan object.
        return Scan(number=number, protocol=protocol, time_series=time_series,
                    bolus_arrival_index=self.bolus_arrival_index,
                    rois=rois, registrations=registrations)
    
    def _create_rois(self, xnat_scan, **opts):
        """
        :param xnat_scan: the XNAT scan object
        :param opts: the following keyword arguments:
        :option color_table: the color table file base name
        :option centroids: the ROI centroids list
        :option average_intensities: the ROI average intentsity list
        :return: the qiprofile Regions list for each lesion in
            lesion number order
        """
        # The ROI resource.
        rsc = xnat_scan.resource(ROI_RESOURCE)
        # The [(mask, label map)] list.
        rois = []
        if not rsc.exists():
            return rois
        # The file object label is the file base name.
        fnames = set(rsc.files().get())
        # The mask files do not have _color in the base name.
        # Sort the mask files by the lesion prefix.
        masks = sorted(f for f in fnames if not '_color' in f)
        centroids = opts.get('centroids')
        avg_intensities = opts.get('average_intensities')
        # The color look-up table.
        color_table = opts.get('color_table')
        # Make the ROIs.
        rois = []
        for i, mask in enumerate(masks):
            roi_opts = lobel_map_opts.clone()
            label_map_name = label_map_basename(mask)
            if label_map_name in fnames:
                # A label map must have a color table.
                if not color_table:
                    raise ImagingUpdateError(
                        "The %s label map %s is missing a color table" %
                        (xnat_path(xnat_scan), label_map_name)
                    )
                label_map = LabelMap(filename=label_map_name,
                                     color_table=color_table)
                roi_opts['label_map'] = label_map
            if centroids and i < len(centroids):
                centroid = centroids[i]
                if centroid:
                    roi_opts['centroid'] = centroid
            if avg_intensities and i < len(avg_intensities):
                avg_intensity = avg_intensities[i]
                if avg_intensity:
                    roi_opts['avg_intensity'] = avg_intensity
            roi = Region(mask, **roi_opts)
            rois.append((mask, label_map))
        
        return rois
    
    def _create_registration(self, resource):
        """
        Makes the qiprofile Registration object from the XNAT resource.
        
        :param resource: the XNAT registration resource object
        :return: the qiprofile Registration object
        """
        # Find or create the registration protocol.
        cfg = self._read_configuration(resource, REG_CONF_FILE)
        technique_section = cfg.get('General')
        if not technique_section:
            raise ImagingUpdateError("The XNAT resource %s protocol is"
                                     " missing the General section" %
                                     xnat_path(resource))
        technique = technique_section.get('technique')
        if not technique:
            raise ImagingUpdateError("The XNAT resource %s protocol is"
                                     " missing the technique option" %
                                     xnat_path(resource))
        key = dict(technique=technique, configuration=cfg)
        protocol = database.get_or_create(RegistrationProtocol, key)
        
        # There must be a time series.
        time_series_file = next(f for f in resource.files() if f.label().ends_with('_ts.nii.gz'))
        time_series = TimeSeries(name=xnat_name(time_series_file))
        
        # Return the new qiprofile Registration object.
        return Registration(protocol=protocol, time_series=time_series,
                            resource=xnat_name(resource))
    
    def _create_modeling(self, resource, scans):
        """
        Creates the qiprofile Modeling object from the given XNAT
        resource object.
        
        :param resource: the modeling source XNAT resource object
        :param scans: the REST Scan list for the current session
        :return: the qiprofile Modeling object
        """
        # The XNAT modeling files.
        xnat_files = resource.files()
        
        # The modeling configuration.
        cfg = self._read_configuration(resource, MODELING_CONF_FILE)
        
        # Tease out the modeling source.
        source_topic = cfg.pop('Source', None)
        if not source_topic:
            raise ImagingUpdateError("The XNAT modeling configuration %s is"
                               " missing the Source topic" % xnat_path(cfg_file))
        source_rsc = source_topic.get('resource')
        if not source_rsc:
            raise ImagingUpdateError("The XNAT modeling configuration %s Source"
                               " topic is missing the resource option" %
                               xnat_path(cfg_file))
        xnat_scan = resource.parent()
        if source_rsc.startswith('reg_'):
            reg_lists = (s.registrations for s in scans)
            regs = concat(*reg_lists)
            reg = next((r for r in regs if r.resource == source_rsc), None)
            if not reg:
                raise ImagingUpdateError("The XNAT modeling resource %s source"
                                         " registration resource %s was not"
                                         " found in the session scans" %
                                         (xnat_path(resource), source_rsc))
            source = Modeling.Source(registration=reg.protocol)
        else:
            scan_nbr = xnat_name(xnat_scan)
            scan = next(s for s in scans if s.number == scan_nbr)
            source = Modeling.Source(scan=scan.protocol)
        
        # The qiprofile ModelingProtocol.
        key = dict(technique='Mock', configuration=cfg)
        protocol = database.get_or_create(ModelingProtocol, key)
        
        # The modeling result files.
        xnat_file_labels = {xnat_name(xnat_file) for xnat_file in xnat_files}
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
                        resource=xnat_name(resource), result=result)
    
    def _read_configuration(self, resource, name):
        """
        :param resource: the XNAT resource configuration file parent
        :param name: the configuration XNAT file name
        :return: the configuration dictionary
        :raise ImagingUpdateError: if the configuration XNAT file was
            not found in the resource
        """
        # The registration configuration.
        cfg_file_finder = (xnat_file for xnat_file in resource.files()
                           if xnat_name(xnat_file) == name)
        xnat_cfg_file = next(cfg_file_finder, None)
        if not xnat_cfg_file:
            raise ImagingUpdateError("The XNAT resource %s does not contain"
                               " the configuration file %s" %
                               (xnat_path(resource), name))
        if xnat_cfg_file:
            cfg_file = xnat_cfg_file.get()
            cfg_dict = dict(read_config(cfg_file))
            return cfg_dict
