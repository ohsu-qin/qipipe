"""
This module resets the :meth:project from ``QIN`` to ``QIN_Test``.
"""

from qipipe.helpers.project import project

# Reset the project name.
project(project() + '_Test')
