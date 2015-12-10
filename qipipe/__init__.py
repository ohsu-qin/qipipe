"""The top-level Quantitative Imaging Pipeline module."""
import os


__version__ = '5.6.4'
"""
The one-based major.minor.patch version based on the
`Fast and Loose Versioning <https://gist.github.com/FredLoney/6d946112e0b0f2fc4b57>`_
scheme. Minor and patch version numbers begin at 1.
"""


CONF_DIR = os.path.join(os.path.dirname(__file__), 'conf')
"""The configuration directory."""
