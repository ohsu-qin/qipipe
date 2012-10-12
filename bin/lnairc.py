#!/usr/bin/env python
"""
Creates well-formed symbolic links to AIRC concatenated DICOM files.
"""

import sys
import os
import getopt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib')))

from tcia import Staging

help_message = """
ln_airc.py options DIR...
Options:
    -d --directory\tThe target directory
    -i --include\tThe image file name pattern
    -p --visit\tThe visit subdirectory pattern
    -t --delta\tThe delta directory
    -f --file\tA file containing the source patient directories
    -q\tSuppress messages
    -v\tPrint informational messages
    -h --help\tPrint this help message
"""


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], 'd:i:p:t:hf:qv', ['directory=', 'include=', 'visit=', 'delta=', 'help', 'file='])
        except getopt.error, msg:
            raise Usage(msg)
        
        target = delta = verbosity = None
        # the staging options
        sopts = {}
        # option processing
        for option, value in opts:
            if option == '-q':
                sopts['verbosity'] = None
            if option == '-v':
                sopts['verbosity'] = 'Info'
            if option in ('-d', '--directory'):
                sopts['target'] = value
            if option in ('-i', '--include'):
                sopts['include'] = value
            if option in ('-p', '--visit'):
                sopts['visit'] = value
            if option in ('-t', '--delta'):
                sopts['delta'] = value
            if option in ('-f', '--file'):
                farg = value
                if farg == '-':
                    fs = sys.stdin
                else:
                    fs = open(farg)
                args[:] = [s.strip() for s in fs.readlines()]
            if option in ('-h', '--help'):
                raise Usage(help_message)
        
        Staging(sopts).link_dicom_files(args)
    
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
        return 2
        
    return 0

if __name__ == '__main__':
    sys.exit(main())
