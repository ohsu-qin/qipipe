import re
from ConfigParser import ConfigParser as Config
import json

import logging
logger = logging.getLogger(__name__)

def read_config(in_file):
    """
    Reads and parses the given configuration file.
    
    @see JSONConfig
    @param in_file: the input configuration file path
    @return: the L{JSONConfig}
    """
    cfg = JSONConfig()
    cfg.read(in_file)
    
    return cfg


class JSONConfigError(Exception):
    pass

class JSONConfig(Config):
    """
    JSONConfig parses a configuration option values as JSON objects.
    Strings are quoted, if necessary.
    A bracked value is parsed as a list.
    A case-insensitive match on C{true} or C{false} is parsed
    as C{True}, resp. C{False}.
    
    For example, given the configuration file C{tuning.cfg} with content:
        
        [Tuning]
        method = FFT
        iterations = [[1, 2], 5]
        two_tailed = false
        threshold = 4.0
    
    then:
        
    >> cfg = JSONConfig('tuning.cfg')
    >> cfg['Tuning']
    {'method': u'FFT', 'iterations': [[1, 2], 5], 'two_tailed': False, 'threshold': 4.0}
    
    @param in_file: the input configuration file path
    @return: the L{JSONConfig}
    """

    LIST_PAT = re.compile("""
        \[      # The left bracket
        (.*)    # The list items
        \]$     # The right bracket
    """, re.VERBOSE)
    """A list string pattern."""
    
    EMBEDDED_LIST_PAT = re.compile("""
        ([^[]*)     # A prefix without the '[' character
        (\[.*\])?   # The embedded list
        ([^]]*)     # A suffix without the ']' character
        $           # The end of the value
    """, re.VERBOSE)
    """A (prefix)(list)(suffix) recognition pattern."""
    
    PARSEABLE_ITEM_PAT = re.compile("""
        (
            true            # The JSON True literal
            | false         # The JSON False literal
            | \d+(\.\d+)?   # A JSON number
            | '.*'          # A single-quoted string
            | \".*\"        # A double-quoted string
        )$                  # The end of the value
        """, re.VERBOSE)
    """A non-list string parseable by JSON."""
    
    DECIMAL_WITH_LEADING_PERIOD_PAT = re.compile('\.\d+$')
    """A decimal with a leading period."""
    
    DECIMAL_WITH_TRAILING_PERIOD_PAT = re.compile('\d+\.$')
    """A decimal with a trailing period."""
    
    def __iter__(self):
        return self.next()
    
    def next(self):
        """
        @return the next (section, {item: value}) tuple
        """
        for s in self.sections():
            yield (s, self[s])
    
    def __contains__(self, section):
        """
        @param: the config section to find
        @return: whether this config has the given section
        """
        return self.has_section(section)
    
    def __getitem__(self, section):
        """
        @param section: the configuration section name
        @return: the section option => value dictionary
        """
        return {name: self._parse_entry(name, value) for name, value in self.items(section)}
    
    def _parse_entry(self, name, s):
        """
        @param name: the option name
        @param s: the option string value to parse
        @return: the parsed JSON value
        @raise ValueError: if the value cannot be parsed
        """
        if s:
            json_value = self._to_json(s)
            try:
                return json.loads(json_value)
            except ValueError:
                logger.error("Cannot load the configuration entry %s: %s parsed as %s" % (name, s, json_value))
                raise
    
    def _to_json(self, s):
        """
        @param s: the input string
        @return: the equivalent JSON string
        """
        # Trivial case.
        if not s:
            return
        # If the input is a boolean, number or already quoted,
        # then we are done.
        if JSONConfig.PARSEABLE_ITEM_PAT.match(s):
            return s
        
        # If the string is a list, then make a quoted list.
        # Otherwise, if the string signifies a boolean, then return the boolean.
        # Otherwise, quote the content.
        if JSONConfig.LIST_PAT.match(s):
            return self._quote_list(s)
        elif JSONConfig.DECIMAL_WITH_LEADING_PERIOD_PAT.match(s):
            return '0' + s
        elif JSONConfig.DECIMAL_WITH_TRAILING_PERIOD_PAT.match(s):
            return s + '0'
        elif s.lower() == 'true':
            return 'true'
        elif s.lower() == 'true':
            return 'true'
        elif s.lower() == 'false':
            return 'false'
        elif s.lower() in ['null', 'none', 'nil']:
            return 'null'
        else:
            return '"%s"' % s
    
    def _quote_list(self, s):
        quoted_items = self._quote_list_content(s[1:-1])
        return "[%s]" % ', '.join(quoted_items)
    
    def _quote_list_content(self, s):
        """
        @param s: the comma-separated items
        @return: the list of quoted items
        """
        pre, mid, post = JSONConfig.EMBEDDED_LIST_PAT.match(s).groups()

        if mid:
            items = []
            if pre:
                items.extend(self._quote_list_content(pre))
            items.append(self._quote_list(mid))
            if post:
                items.extend(self._quote_list_content(post))
            return items
        else:
            # No embedded list.
            items = re.split('\s*,\s*', s)
            quoted_items = [self._to_json(item) for item in items if item]
            return quoted_items
        
        