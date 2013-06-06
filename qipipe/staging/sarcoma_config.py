import os
from ConfigParser import ConfigParser as Config

_CFG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'conf', 'sarcoma.cfg')

def sarcoma_location(subject):
    """
    :param subject: the XNAT Subject ID
    :return: the subject tumor location
    """

    return sarcoma_config().get('Tumor Location', subject)

def sarcoma_config():
    """
    :return: the sarcoma configuration
    :rtype: ConfigParser
    """
    if not hasattr(sarcoma_config, 'instance'):
        sarcoma_config.instance = Config()
        sarcoma_config.instance.read(_CFG_FILE)
    
    return sarcoma_config.instance
