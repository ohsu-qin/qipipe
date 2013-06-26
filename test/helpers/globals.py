"""
The unit test global constants.
This module resets the qipipe PROJECT from ``QIN`` to ``QIN_Test``.
"""

from qipipe.helpers import globals as _globals
_globals.PROJECT += '_Test'
from qipipe.helpers.globals import PROJECT
