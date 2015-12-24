import os
import csv
from qiutil.ast_config import (read_config, ASTConfig)


def create_profile(cfg_file, resource, sections, **opts):
    """
    Creates a metadata profile from the given configuration.
    The profile includes the given sections from the input
    configuration file and keyword arguments. A keyword
    item overrides a matching input configuration item. The
    resulting profile is written to a new file.

    :param cfg_file: the modeling configuration file location
    :param resource: the XNAT resource containing the time series
    :param sections: the configuration profile sections
    :param opts: additional {section: {option: value}} items, as
        well as the following keyword arguments:
    :option dest_file: the target profile location
        (default is the input configuration file base name in the
        current directory)
    :return: the destination file location
    """

    # The config options to exclude in the profile.
    EXCLUDED_OPTS =  {'plugin_args', 'run_without_submitting'}

    # The destination option.
    dest_file_opt = opts.pop('dest_file', None)
    # The input config.
    cfg = read_config(cfg_file)
    # Populate the profile.
    profile = ASTConfig()
    # Add the parameter sections.
    for section in sections:
        if cfg.has_section(section):
            # The profile {key, value} dictionary.
            items = {opt: val for opt, val in cfg.items(section)
                     if opt not in EXCLUDED_OPTS}
            if items:
                profile.add_section(section)
                for opt, val in items.iteritems():
                    profile.set(section, opt, val)
    # The keyword arguments override the config file.
    for key, items in opts.iteritems():
        # Topics are capitalized.
        section = key.capitalize()
        if not profile.has_section(section):
            profile.add_section(section)
        for opt, val in items.iteritems():
            profile.set(section, opt, val)

    # Save the profile.
    if dest_file_opt:
        dest_file = os.path.abspath(dest_file_opt)
    else:
        _, base = os.path.split(cfg_file)
        dest_file = os.path.join(os.getcwd(), base)
    with open(dest_file, 'w') as f:
        profile.write(f)

    return dest_file
