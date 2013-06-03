import re
from ConfigParser import ConfigParser as Config
import json

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

    LIST_PAT = re.compile('\[(.*)\]$')
    """A list string pattern."""
    
    EMBEDDED_LIST_PAT = re.compile('([^[]*)(\[.*\])?([^]]*)$')
    """A (prefix)(list)(suffix) recognition pattern."""
    
    PARSEABLE_ITEM_PAT = re.compile("true|false|\.\d+|\d+(\.(\d*))?|'.*'|\".*\"$")
    """A non-list string parseable by JSON."""
    
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
        return {name: self._parse_value(value) for name, value in self.items(section)}
    
    def _parse_value(self, s):
        """
        @param s: the string value to parse
        @return: the parsed JSON value
        """
        if s:
            json_value = self._add_quotes(s)
            return json.loads(json_value)
    
    def _add_quotes(self, s):
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
        elif s.lower() == 'true':
            return True
        elif s.lower() == 'false':
            return False
        else:
            return '"' + s + '"'
    
    def _quote_list(self, s):
        quoted_items = self._quote_list_content(s[1:-1])
        return '[' + ' ,'.join(quoted_items) + ']'
    
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
            quoted_items = [self._add_quotes(item) for item in items if item]
            return quoted_items
        
        