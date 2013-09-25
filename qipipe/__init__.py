"""The ``qipipe`` module includes the QIN Python source modules."""

from .helpers.project import project
from . import (helpers, interfaces, pipeline, staging)

__version__ = '3.2.2'
"""The one-based major.minor.patch version."""
