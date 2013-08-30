import os
import logging
import logging.config
import yaml


def logger(name):
    """
    This method is the preferred way to obtain a logger.
    
    Example:
    >>> from qipipe.helpers.logging_helper import logger
    >>> logger(__name__).debug("Starting my application...")
    
    :param name: the caller's context ``__name__``
    :return: the Python Logger instance
    """
    # Configure on demand.
    if not hasattr(logger, 'configured'):
        configure()

    return logging.getLogger(name)


def configure(cfg_file=None, **opts):
    """
    Configures the global logger. The logging configuration is obtained from
    from the given keyword options and the YAML_ logging configuration files.
    The following logging configuration files are loaded in low-to-high
    precedence:
    
    - the ``conf/logging.yaml`` source distribution file
    
    - the ``logging.yaml`` file in the current directory
    
    - the file specified by the ``QIN_LOG_CFG`` environment variable
    
    - the *cfg_file* parameter
    
    The ``opts`` keyword arguments specify simple logging parameters that
    override the configuration file settings.
    
    .. _YAML: http://www.yaml.org
 
    The logging configuration file ``formatters``, ``handlers`` and
    ``loggers`` sections are updated incrementally. For example, the
    ``conf/logging.yaml`` source distribution file defines the ``default``
    formatter ``format`` and ``datefmt``. If the  ``logging.yaml`` file in
    the current directory overrides the ``format`` but not the ``datefmt``,
    then the default ``datefmt`` is retained rather than unset. Thus, a custom
    logging configuration file need define only the settings which override
    the default configuration.
    
    The default logger writes ``INFO`` level messages to a rotating
    ``log/qipipe.log`` log file and ``ERROR`` level messages to the
    console. If the file handler is enabled, then this
    :meth:`qipipe.helpers.logging_helper.config` method ensures
    that the log file parent directory exists.
    
    Examples:
    
    - Write to the log:
      
      >>> from qipipe.helpers.logging_helper import logger
      >>> logger(__name__).debug("Started the application...")
      
      or, in a class instance:
      
      >>> from qipipe.helpers.logging_helper import logger
      >>> class MyApp(object):
      ...     def __init__(self):
      ...         self._logger = logger(__name__)
      ...     def start(self):
      ...         self._logger.debug("Started the application...")
    
    - Write debug messages to the file log:
      
      >>> from qipipe.helpers import logging_helper
      >>> logging_helper.configure(level='DEBUG')
    
    - Set the log file:
      
      >>> from qipipe.helpers import logging_helper
      >>> logging_helper.configure(filename='log/myapp.log')
    
    - Define your own logging configuration:
      
      >>> from qipipe.helpers import logging_helper
      >>> logging_helper.configure('/path/to/my/conf/logging.yaml')
    
    - Simplify the console log message format::
        
        ./logging.yaml:
        ---
        formatters:
          simple:
            format: '%(name)s - %(message)s'
        
        handlers:
          console:
            formatter: simple
    
    :param cfg_file: the optional custom configuration YAML file
    :param opts: the logging configuration options, including
        the following short-cuts:
    :keyword filename: the log file path
    :keyword level: the file handler log level
    """
    # Load the configuration files.
    cfg = _load_config(cfg_file)

    # The options override the configuration files.
    if 'filename' in opts:
        # TODO - document this
        fname = opts.pop('filename')
        if fname:
            cfg['handlers']['file_handler']['filename'] = fname
        else:
            cfg['handlers']['file_handler']['filename'] = '/dev/null'
            cfg['loggers']['qipipe']['handlers'] = ['console']
    if 'level' in opts:
        # The log level is set in both the logger and the handler,
        # and the more restrictive level applies. Therefore, set
        # the log level in both places.
        level = opts.pop('level')
        cfg['loggers']['qipipe']['level'] = level
        if 'file_handler' in cfg['loggers']['qipipe']['handlers']:
            cfg['handlers']['file_handler']['level'] = level
        else:
            cfg['handlers']['console']['level'] = level
    # Add the other options, if any.
    _update_config(cfg, opts)

    # Make the log file parent directory, if necessary.
    if 'file_handler' in cfg['loggers']['qipipe']['handlers']:
        path = cfg['handlers']['file_handler']['filename']
        log_dir = os.path.dirname(path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

    # Configure the logger.
    logging.config.dictConfig(cfg)

    # Set the logger configured flag.
    setattr(logger, 'configured', True)


LOG_CFG_ENV_VAR = 'QIN_LOG_CFG'
"""The user-defined environment variable logging configuration path."""

LOG_CFG_FILE = 'logging.yaml'
"""The optional current working directory logging configuration file name."""

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The ``qipipe`` distribution directory."""

APP_LOG_CFG_PATH = os.path.join(BASE_DIR, 'conf', LOG_CFG_FILE)
"""The default application logging configuration file path."""


def _load_config(cfg_file=None):
    """
    Loads the logger configuration files, as described in
    :meth:`qipipe.helpers.logging.configure`.
 
    :return: the logging configuration dictionary
    :raises ValueError: if the configuration file argument is specfied but
        does not exist
    """
    config = _load_config_file(APP_LOG_CFG_PATH)
    env_cfg_file = os.getenv(LOG_CFG_ENV_VAR, None)
    if env_cfg_file and os.path.exists(env_cfg_file):
        env_cfg = _load_config_file(env_cfg_file)
        _update_config(config, env_cfg)
    if os.path.exists(LOG_CFG_FILE):
        cwd_cfg = _load_config_file(LOG_CFG_FILE)
        _update_config(config, cwd_cfg)
    if cfg_file:
        if os.path.exists(cfg_file):
            arg_cfg = _load_config_file(cfg_file)
            _update_config(config, arg_cfg)
        else:
            raise ValueError("Configuration file not found: %s" % cfg_file)

    return config


def _load_config_file(path):
    """
    Loads the given logger configuration file.
 
    :param: path: the file path
    :return: the parsed configuration parameter dictionary
    """
    with open(path) as fs:
        return yaml.load(fs)


def _update_config(config, other):
    """
    Updates the given logging configuration from another configuration.
    The ``'formatters``, ``handlers`` and ``loggers`` sections are
    updated recursively, e.g. a formatter in the base configuration
    is not deleted if it is not in the replacement configuration.
    
    :param config: the logging configuration dictionary to update
    :param other: the replacement logging configuration dictionary
    """
    recursive = ['formatters', 'handlers', 'loggers']
    for section in other:
        if section in recursive and section in config:
            cfg_section = config[section]
            other_section = other[section]
            for subsection in other_section:
                if subsection in cfg_section:
                    cfg_section[subsection].update(other_section[subsection])
                else:
                    cfg_section[subsection][subsection] = other[subsection]
        else:
            config[section] = other[section]
