import os
from six.moves.configparser import ConfigParser as Config
from six.moves.configparser import NoOptionError

CFG_FILE = os.path.abspath(
    os.path.join( os.path.dirname(__file__), '..', 'conf', 'sarcoma.cfg')
)
"""
The Sarcoma Tumor Location configuration file. This file contains
properties that associat the subject name to the location, e.g.::
    
    Sarcoma004 = SHOULDER

The value is the SNOMED anatomy term.
"""


class ConfigError(Exception):
    pass


def sarcoma_location(subject):
    """
    :param subject: the XNAT Subject ID
    :return: the subject tumor location
    """
    try:
      return sarcoma_config().get('Tumor Location', subject)
    except NoOptionError:
      raise ConfigError("Tumor location for subject %s was not found in the"
                        " sarcoma configuration file %s" % (subject, CFG_FILE))


def sarcoma_config():
    """
    :return: the sarcoma configuration
    :rtype: ConfigParser
    """
    # Read the configuration file on demand.
    if not hasattr(sarcoma_config, 'instance'):
        sarcoma_config.instance = Config()
        sarcoma_config.instance.read(CFG_FILE)
    
    return sarcoma_config.instance
