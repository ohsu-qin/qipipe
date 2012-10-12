#!/usr/bin/env python
"""
Prints the CTP patient id mapping file for Patient ID tag values in DICOM files.
"""

import sys
import os
import getopt
import re
import dicom


help_message = """
ctp_map.py options DIR...
Options:
    -p --prefix\tThe target TCIA patient id prefix
    -h --help\tPrint this help message
"""


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

class OptError(RuntimeError):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], 'hp:v', ['help', 'prefix='])
        except getopt.error, msg:
            raise Usage(msg)
        
        # option processing
        prefix = None
        for option, value in opts:
            if option == '-v':
                verbose = True
            if option in ('-p', '--prefix'):
                prefix = value
            if option in ('-h', '--help'):
                raise Usage(help_message)
        if prefix == None:
            raise OptError('Required prefix option was not set.')

        print_ctp_map(prefix, args)
    
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
        return 2
    
    return 0
    
def print_ctp_map(prefix, dirs):
    """Prints the CTP map for the DICOM files in the given directories"""
    # The ID lookup entry format.
    fmt = "ptid/%(patient id)s=" + prefix + "%(patient number)04d"
    # The RE to extract the patient number suffix.
    pat = re.compile('\d+$')
    for d in dirs:
        # The patient number is extracted from the directory name.
        pnt_match = pat.search(os.path.basename(d))
        if not pnt_match:
            continue
        pnt_nbr = int(pnt_match.group(0))
        # The patient ids for this patient.
        pnt_ids = set()
        for root, subdirs, files in os.walk(d):
            for f in files:
                path = os.path.join(root, f)
                # Read the DICOM file with defer_size=None, stop_before_pixels=True and force=False.
                ds = dicom.read_file(path, 256, True, False)
                pnt_ids.add(ds.PatientID)
        for pnt_id in pnt_ids:
            # Escape colon and blank in the source patient id.
            esc_pnt_id = pnt_id.replace(':', '\:').replace(' ', '\ ')
            print >> sys.stdout, fmt % {'patient id': esc_pnt_id, 'patient number': pnt_nbr}

if __name__ == '__main__':
    sys.exit(main())
