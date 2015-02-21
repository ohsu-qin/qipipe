import os
import re
import glob
from setuptools import (setup, find_packages)

VCS_RQMT_PAT = re.compile('^\w+\+\w+:')
"""
The pattern for detecting a VCS requirement spec, e.g.
``git+git://...``.
"""

def version(package):
    """
    Return package version as listed in the `__init.py__` `__version__`
    variable.
    """
    init_py = open(os.path.join(package, '__init__.py')).read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


def requires():
    """
    @return: the ``requirements.txt`` package specifications
    
    :Note: ``pip`` supports VCS package specifications, but
       setup.py does not. Therefore, this method filters out
       the VCS requirements in ``requirements.txt``. The VCS
       dependencies must be installed separately as described
       in the User Guide **Installation** section.
    """
    with open('requirements.txt') as f:
        rqmts = f.read().splitlines()
        return [rqmt for rqmt in rqmts if not VCS_RQMT_PAT.match(rqmt)]
        

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
    include_package_data = True,
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
    install_requires = requires()
)
