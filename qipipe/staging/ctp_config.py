import os
from ConfigParser import ConfigParser as Config

_CFG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'conf', 'ctp.cfg')

def ctp_study(collection):
    """
    @param collection: the image collection
    @return: the CTP study name
    """
    return ctp_config().get('Study', collection)

def ctp_config():
    if not hasattr(ctp_config, 'instance'):
        ctp_config.instance = Config()
        ctp_config.instance.read(_CFG_FILE)
    return ctp_config.instance