import os
import csv
from collections import Counter
from qiutil.ast_config import (read_config, ASTConfig)

EXCLUDED_OPTS =  {'plugin_args', 'run_without_submitting'}
"""The config options to exclude in the profile."""


class MetadataError(Exception):
    """Metadata parsing error."""
    pass


def create_profile(configuration, sections, dest, **opts):
    """
    Creates a metadata profile from the given configuration.
    The configuration input is a {section: {option: value}}
    dictionary. The target profile is a Python configuration
    file which includes the given sections. The section content
    is determined by the input configuration and the keyword
    arguments. The keyword item overrides a matching input
    configuration item. The resulting profile is written to a
    new file.
    
    :param configuration: the configuration dictionary
    :param sections: the target profile sections
    :param dest: the target profile file location
    :param opts: additional {section: {option: value}} items
    :return: the target file location
    """
    
    # The target profile to populate.
    profile = ASTConfig()
    # MongoDB does not allow dotted dictionary keys.
    section_map = {section: section.split('.')[-1] for section in sections}
    section_counts = Counter(section_map.itervalues())
    dups = [section for section, count in section_counts.items()
            if count > 1]
    if dups:
        raise MetadataError("The profile configuration has duplicate option"
                            " base names: %s" % dups)
    
    # Add the input parameter sections.
    for cfg_section, prf_section in section_map.iteritems():
        cfg_items = configuration.get(cfg_section)
        if cfg_items:
            # The profile {option, value} dictionary.
            prf_items = {opt: val for opt, val in cfg_items.iteritems()
                         if opt not in EXCLUDED_OPTS}
            if prf_items:
                profile.add_section(prf_section)
                for opt, val in prf_items.iteritems():
                    profile.set(prf_section, opt, val)
    
    # The keyword arguments override the config file.
    for key, items in opts.iteritems():
        # Topics are capitalized.
        section = key.capitalize()
        if not profile.has_section(section):
            profile.add_section(section)
        for opt, val in items.iteritems():
            profile.set(section, opt, val)
    
    # Save the profile.
    dest = os.path.abspath(dest)
    dest_dir = os.path.dirname(dest)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    with open(dest, 'w') as f:
        profile.write(f)
    
    return dest
