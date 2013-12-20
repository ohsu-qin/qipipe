import os


def default_configuration():
    """
    Returns the XNAT configuration file location determined as the first file
    found in the following precedence order:
    
    1. ``xnat.cfg`` in the home ``.xnat`` subdirectory
    
    2. ``xnat.cfg`` in the home directory
    
    3. ``xnat.cfg`` in the ``/etc`` directory

    :return: the configuration location, if any
    """
    for f in [DOT_CFG, HOME_CFG, ETC_CFG]:
        if os.path.exists(f):
            return f

DOT_CFG = os.path.join(os.path.expanduser('~'), '.xnat', 'xnat.cfg')
"""The XNAT home ``.xnat`` subdirectory configuration location."""

HOME_CFG = os.path.join(os.path.expanduser('~'), 'xnat.cfg')
"""The XNAT home configuration location."""

ETC_CFG = os.path.join('/etc', 'xnat.cfg')
"""The Linux global ``/etc`` XNAT configuration location."""
