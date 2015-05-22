"""
Image processing preparation.

The staging package defines the functions used to prepare the study image files
for import into XNAT, submission to the TCIA QIN collections and pipeline
processing.
"""

# OHSU - The ohsu module creates the OHSU QIN collections.
# TODO - this should be a config item.
from . import ohsu
