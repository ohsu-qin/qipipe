"""Command qipipe command options."""

import qiutil
from qiutil.command import add_options


def configure_log(**opts):
    """
    Configure the logger for the qi* modules.
    """
    qiutil.command.configure_log('qipipe', 'qixnat', 'qidicom', 'qiutil',
                                 **opts)
