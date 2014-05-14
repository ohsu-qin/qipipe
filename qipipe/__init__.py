"""The top-level Qunatitative Imaging Pipeline module."""
from pkg_resources import get_distribution
from .helpers.project import project
from . import (helpers, interfaces, pipeline, staging)

__version__ = '4.2.1'
"""
The one-based major.minor.patch version.
The version numbering scheme loosely follows http://semver.org/.
The major version is incremented when there is an incompatible
public API change. The minor version is incremented when there
is a backward-compatible functionality change. The patch version
is incremented when there is a backward-compatible refactoring
or bug fix. All major, minor and patch version numbers begin at
1.
"""
