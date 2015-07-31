import os
from six.moves.configparser import ConfigParser as Config

CFG_FILE = os.path.join(
    os.path.dirname(__file__), '..', 'conf', 'ctp.cfg')


def ctp_collection_for_name(name):
    """
    :param name: the QIN collection name
    :return: the CTP collection name
    """
    return ctp_config().get('CTP', name)


def ctp_config():
    if not hasattr(ctp_config, 'instance'):
        ctp_config.instance = Config()
        ctp_config.instance.read(CFG_FILE)
    return ctp_config.instance
