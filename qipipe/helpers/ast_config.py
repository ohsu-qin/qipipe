import os, re, ast
from ConfigParser import ConfigParser as Config
from qipipe.helpers.collection_helper import to_series

import logging
logger = logging.getLogger(__name__)

def read_config(*filenames):
    """
    Reads and parses the given configuration file.
    
    :param filenames: the input configuration file names
    :return: the configuration
    :rtype: :class:`qipipe.helpers.ast_config.ASTConfig`
    :raise ValueError: if none of the files could not be read
    """
    cfg = ASTConfig()
    cfg_files = cfg.read(filenames)
    if not cfg_files:
        raise ValueError("Configuration file could not be read: %s" % filenames)
    logger.debug("Loaded configuration from %s with sections %s." %
        (to_series(cfg_files), to_series(cfg.sections())))
    
    return cfg


class ASTConfig(Config):
    """
    ASTConfig parses a configuration option values as AST objects.
    Strings are quoted, if necessary.
    A bracked value is parsed as a list.
    A case-insensitive match on ``true`` or ``false`` is parsed
    as ``True``, resp. ``False``.
    
    For example, given the configuration file ``tuning.cfg`` with content::
        
        [Tuning]
        method = FFT
        iterations = [[1, 2], 5]
        parameters = [(1,), (2, 3)]
        two_tailed = false
        threshold = 4.0
    
    then:
    
    >>> cfg = ASTConfig('tuning.cfg')
    >>> cfg['Tuning']
    {'method': u'FFT', 'parameters' = [(1,), (2, 3)], 'iterations': [[1, 2], 5],
    'two_tailed': False, 'threshold': 4.0}
    """

    LIST_PAT = re.compile("""
        \[      # The left bracket
        (.*)    # The list items
        \]$     # The right bracket
        """, re.VERBOSE)
    """A list string pattern."""

    TUPLE_PAT = re.compile("""
        \(      # The left paren
        (.*)    # The tuple items
        \)$     # The right paren
        """, re.VERBOSE)
    """A tuple string pattern."""
    
    EMBEDDED_LIST_PAT = re.compile("""
        ([^[]*)     # A prefix without the '[' character
        (\[.*\])?   # The embedded list
        ([^]]*)     # A suffix without the ']' character
        $           # The end of the value
        """, re.VERBOSE)
    """A (prefix)(list)(suffix) recognition pattern."""
    
    EMBEDDED_TUPLE_PAT = re.compile("""
        ([^(]*)     # A prefix without the '(' character
        (\(.*\))?   # The embedded tuple
        ([^)]*)     # A suffix without the ')' character
        $           # The end of the value
        """, re.VERBOSE)
    """A (prefix)(tuple)(suffix) recognition pattern."""
    
    PARSEABLE_ITEM_PAT = re.compile("""
        (
            True            # The True literal
            | False         # The False literal
            | \.\d+         # A decimal with leading period
            | \d+\.         # A decimal with trailing period
            | \d+(\.\d+)?   # A number
            | \d+\.e[+-]\d+ # A floating point
            | \'.*\'        # A single-quoted string
            | \".*\"        # A double-quoted string
        )$                  # The end of the value
        """, re.VERBOSE)
    """A non-list string parseable by AST."""
    
    def __iter__(self):
        return self.next()
    
    def next(self):
        """
        :yield: the next (section, {item: value} dictionary) tuple
        """
        for s in self.sections():
            yield (s, self[s])
    
    def __contains__(self, section):
        """
        :param: the config section to find
        :return: whether this config has the given section
        """
        return self.has_section(section)
    
    def __getitem__(self, section):
        """
        :param section: the configuration section name
        :return: the section option => value dictionary
        """
        return {name: self._parse_entry(name, value) for name, value in self.items(section)}
    
    def _parse_entry(self, name, s):
        """
        :param name: the option name
        :param s: the option string value to parse
        :return: the parsed AST value
        :raise SyntaxError: if the value cannot be parsed
        """
        if s:
            ast_value = self._to_ast(s)
            try:
                return ast.literal_eval(ast_value)
            except SyntaxError:
                logger.error("Cannot load the configuration entry %s: %s parsed as %s" % (name, s, ast_value))
                raise
    
    def _to_ast(self, s):
        """
        :param s: the input string
        :return: the equivalent AST string
        """
        # Trivial case.
        if not s:
            return
        # If the input is a boolean, number or already quoted,
        # then we are done.
        if ASTConfig.PARSEABLE_ITEM_PAT.match(s):
            return s
        
        # If the string is a list, then make a quoted list.
        # Otherwise, if the string signifies a boolean, then return the boolean.
        # Otherwise, quote the content.
        if ASTConfig.LIST_PAT.match(s) or ASTConfig.TUPLE_PAT.match(s):
            return self._quote_list_or_tuple(s)
        elif s.lower() == 'true':
            return 'True'
        elif s.lower() == 'false':
            return 'False'
        elif s.lower() in ['null', 'none', 'nil']:
            return 'None'
        else:
            return '"%s"' % s.replace('"', '\\"')
    
    def _quote_list_or_tuple(self, s):
        quoted_items = self._quote_list_content(s[1:-1])
        if len(quoted_items) == 1:
            quoted_items.append('')
        return s[0] + ', '.join(quoted_items) + s[-1]
    
    def _quote_list_content(self, s):
        """
        :param s: the comma-separated items
        :return: the list of quoted items
        """
        pre, mid, post = ASTConfig.EMBEDDED_LIST_PAT.match(s).groups()
        if not mid:
            pre, mid, post = ASTConfig.EMBEDDED_TUPLE_PAT.match(s).groups()
        if mid:
            items = []
            if pre:
                items.extend(self._quote_list_content(pre))
            # Balance the left and right bracket or paren.
            left = mid[0]
            right = mid[-1]
            count = 1
            i = 1
            while (count > 0):
                if mid[i] == left:
                    count = count + 1
                elif mid[i] == right:
                    count = count - 1
                i = i + 1
            post = mid[i:] + post
            mid = mid[0:i]
            items.append(self._quote_list_or_tuple(mid))
            if post:
                items.extend(self._quote_list_content(post))
            return items
        else:
            # No embedded list.
            items = re.split('\s*,\s*', s)
            quoted_items = [self._to_ast(item) for item in items if item]
            return quoted_items
        
        