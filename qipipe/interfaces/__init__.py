"""
The ``interfaces`` module includes the custom QIN Nipype interface classes.
"""

from .compress import Compress
from .copy import Copy
from .fastfit import Fastfit
from .fix_dicom import FixDicom
from .gate import Gate
from .group_dicom import GroupDicom
from .lookup import Lookup
from .map_ctp import MapCTP
from .move import Move
from .mri_volcluster import MriVolCluster
from .unpack import Unpack
from .uncompress import Uncompress
from .xnat_download import XNATDownload
from .xnat_upload import XNATUpload
from .xnat_find import XNATFind
from .update_qiprofile import UpdateQIProfile