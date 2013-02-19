import os
import pyxnat

_XNAT_DOT_CFG = os.path.join(os.path.expanduser('~'), '.xnat', 'xnat.cfg')
"""The XNAT home C{.xnat} subdirectory configuration location."""

_XNAT_HOME_CFG = os.path.join(os.path.expanduser('~'), 'xnat.cfg')
"""The XNAT home configuration location."""

_XNAT_ETC_CFG = os.path.join('/etc', 'xnat.cfg')
"""The Linux global C{/etc} XNAT configuration location."""

class XNATError(Exception):
    pass

def config():
    """
    Returns the XNAT configuration file location determined as the first file found
    in the following precedence order:
        1. C{xnat.cfg} in the home C{.xnat} subdirectory
        2. C{xnat.cfg} in the home directory
        3. C{xnat.cfg} in the C{/etc} directory
    
    @return: the configuration location, if any
    """
    for f in [_XNAT_DOT_CFG, _XNAT_HOME_CFG, _XNAT_ETC_CFG]:
        if os.path.exists(f):
            return f

def subject_id_for_label(project, label, create=False):
    """
    @param project: the XNAT project
    @param label: the subject label
    @param create: flag indicating whether to create the subject if it does not yet exist
    @return: the subject id
    @raise XNATError: if the project does not exist
    """
    
    xnat = pyxnat.Interface(config=config())
    p = xnat.select.project(project)
    if not p.exists():
        raise XNATError("Project not found: %s" % project)
    s = p.subject(label)
    if not s.exists():
        if create:
            s.insert()
        else:
            return None
    return s.id()
