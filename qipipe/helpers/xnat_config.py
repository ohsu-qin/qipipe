import os

__all__ = ['default_configuration']


def default_configuration():
    """
    Returns the XNAT configuration file location determined as the first file
    found in the following precedence order:
    
    1. The ``XNAT_CFG`` environment variable, if it is set.
    
    1. ``xnat.cfg`` in the current working directory
    
    2. ``xnat.cfg`` in the home ``.xnat`` subdirectory
    
    3. ``xnat.cfg`` in the home directory
    
    4. ``xnat.cfg`` in the ``/etc`` directory

    :return: the configuration location, if any
    """
    cfgs = [CWD_CFG, DOT_CFG, HOME_CFG, ETC_CFG]
    env_cfg = os.getenv('XNAT_CFG')
    if env_cfg:
        cfgs.insert(0, env_cfg)
    for f in cfgs:
        if os.path.exists(f):
            return f

CWD_CFG = os.path.join(os.getcwd(), 'xnat.cfg')
"""The XNAT current directory configuration location."""

DOT_CFG = os.path.join(os.path.expanduser('~'), '.xnat', 'xnat.cfg')
"""The XNAT home ``.xnat`` subdirectory configuration location."""

HOME_CFG = os.path.join(os.path.expanduser('~'), 'xnat.cfg')
"""The XNAT home configuration location."""

ETC_CFG = os.path.join('/etc', 'xnat.cfg')
"""The Linux global ``/etc`` XNAT configuration location."""
