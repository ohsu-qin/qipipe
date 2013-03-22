import os

__all__ = ['default_configuration']

def default_configuration():
    """
    Returns the XNAT configuration file location determined as the first file found
    in the following precedence order:
        1. C{xnat.cfg} in the home C{.xnat} subdirectory
        2. C{xnat.cfg} in the home directory
        3. C{xnat.cfg} in the C{/etc} directory

    @return: the configuration location, if any
    """
    for f in [DOT_CFG, HOME_CFG, ETC_CFG]:
        if os.path.exists(f):
            return f

DOT_CFG = os.path.join(os.path.expanduser('~'), '.xnat', 'xnat.cfg')
"""The XNAT home C{.xnat} subdirectory configuration location."""

HOME_CFG = os.path.join(os.path.expanduser('~'), 'xnat.cfg')
"""The XNAT home configuration location."""

ETC_CFG = os.path.join('/etc', 'xnat.cfg')
"""The Linux global C{/etc} XNAT configuration location."""