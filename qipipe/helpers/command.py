"""Command qipipe command options."""

import qiutil

def configure_log(**opts):
    # Configure the logger for this qixnat module and the qiutil module.
    qiutil.command.configure_log('qipipe', 'qixnat', 'qidicom', 'qiutil', **opts)
