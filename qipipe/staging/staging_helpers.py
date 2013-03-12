"""Pipeline utility functions."""

import re

_SSS_REGEX = '(\w+\d{2})/(session\d{2})/(series\d{3})'
"""The subject/session/series regexp pattern."""

def match_session_hierarchy(path):
    """
    Matches the subject, session and series names from the given input path.
    
    @param path: the path to match
    @return: the matching (subject, session, series) tuple, or None if no match
    """
    match = re.search(_SSS_REGEX, path)
    if match:
        return match.groups()
