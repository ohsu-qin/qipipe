"""The top-level Quantitative Imaging Pipeline module."""

__version__ = '4.5.3'
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

def project(name=None):
    """
    Gets or sets the current XNAT project name.
    The default project name is ``QIN``.
    
    :param name: the XNAT project name to set, or None to get the
        current project name
    :return: the current XNAT project name
    """
    if name:
        project.name = name
    elif not hasattr(project, 'name'):
        project.name = None

    return project.name or 'QIN'
