import os
import io
from ConfigParser import ConfigParser as Config

_CFG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'conf', 'sarcoma.cfg')

def sarcoma_location(pt_id):
    return sarcoma_config().get('Tumor Location', pt_id)

def sarcoma_config():
    if not hasattr(sarcoma_config, 'instance'):
        sarcoma_config.instance = Config()
        sarcoma_config.instance.read(_CFG_FILE)
    return sarcoma_config.instance
