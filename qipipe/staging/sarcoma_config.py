import os
import ConfigParser

_CFG_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'conf', 'sarcoma.cfg')
_CONFIG = ConfigParser()
_CONFIG.read(_CFG_FILE)

def sarcoma_location(pt_id):
    return _CONFIG.get('Tumor Location', pt_id)
