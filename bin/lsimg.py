#!/usr/bin/env python
"""
Prints each patient-visit-series-acquisition-image path in a DICOM image hierarchy.
The format of each path is a line of tab-delimited fields.
"""

import sys
import os
import getopt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib')))

from helpers import dicom_tags


help_message = """
lsimg.py options FILE...
Options:
    -v\tPrint informational messages
    -h --help\tPrint this help message
"""


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    summary = False
    try:
        try:
            opts, args = getopt.getopt(argv[1:], 'hv', ['help'])
        except getopt.error, msg:
            raise Usage(msg)
        
        # option processing
        for option, value in opts:
            if option == '-v':
                verbose = True
                if option in ('-h', '--help'):
                    raise Usage(help_message)
        
        # Build the hierarchy.
        hierarchy = dicom_tags.read_image_hierarchy(*args)
        # Print each patient-visit-series-acquisition-image field.
        for path in hierarchy:
            print "\t".join([str(item) for item in path])
        
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
        return 2
    
    return 0        

if __name__ == '__main__':
    sys.exit(main())
