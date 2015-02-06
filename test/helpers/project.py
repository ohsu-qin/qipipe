"""
This module resets the :meth:qipipe.project` from ``QIN`` to ``QIN_Test``.
"""

from qipipe import project

# Reset the project name.
project(project() + '_Test')
