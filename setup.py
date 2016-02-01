import os
import re
import glob
import warnings
from setuptools import (setup, find_packages)

VCS_RQMT_PAT = re.compile('^\w+\+\w+:')
"""
The pattern for detecting a VCS requirement spec, e.g.
``git+git://...``.
"""


class InstallError(Exception):
    """qipipe installation error."""
    pass


def version(package):
    """
    :return: the package version as listed in the package `__init.py__`
        `__version__` variable.
    """
    # The version string parser.
    REGEXP = re.compile("""
       __version__   # The version variable
       \s*=\s*       # Assignment
       ['\"]         # Leading quote
       (.+)          # The version string capture group
       ['\"]         # Trailing quote
    """, re.VERBOSE)
    
    with open(os.path.join(package, '__init__.py')) as f:
       match = REGEXP.search(f.read())
       if not match:
           raise InstallError("The Nipype __version__ variable was not found")
       return match.group(1)


def requires():
    with open('requirements.txt') as f:
        return f.read().splitlines()


def dependency_links():
    """
    Returns the non-PyPI ``qipipe`` requirements in
    ``constraints.txt`` which match the :const:`VCS_RQMT_PAT`
    pattern.
    
    :return: the non-PyPI package specifications
    """
    with open('constraints.txt') as f:
        rqmts = f.read().splitlines()
        # Match on git dependency links. Not a general solution, but
        # good enough so far.
        # TODO - revisit if Python 3 settles on a sane package manager.
        return [rqmt for rqmt in rqmts if VCS_RQMT_PAT.match(rqmt)]


def readme():
    with open("README.rst") as f:
        return f.read()


setup(
    name = 'qipipe',
    version = version('qipipe'),
    author = 'OHSU Knight Cancer Institute',
    author_email = 'loneyf@ohsu.edu',
    platforms = 'Any',
    license = 'MIT',
    keywords = 'Imaging QIN OHSU DCE MR XNAT Nipype',
    packages = find_packages(exclude=['test**']),
    package_data = dict(qipipe=['conf/*.cfg']),
    scripts = glob.glob('bin/*'),
    url = 'http://qipipe.readthedocs.org/en/latest/',
    description = 'Quantitative Imaging Profile pipeline',
    long_description = readme(),
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    install_requires = requires(),
    dependency_links = dependency_links()
)
