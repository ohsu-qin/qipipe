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


def version(package):
    """
    :return: the package version as listed in the `__init.py__`
        `__version__` variable.
    """
    init_py = open(os.path.join(package, '__init__.py')).read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


def requires():
    """
    Returns the PyPI ``qipipe`` requirements in ``requirements.txt``
    which don't match the :const:`VCS_RQMT_PAT` pattern.
    
    :Note: ``pip`` supports VCS package specifications, but
       setup.py does not. Therefore, this method filters out
       the requirements in ``requirements.txt`` entries which
       match the :const:`VCS_RQMT_PAT` pattern. The VCS
       dependencies must be installed separately as described
       in the User Guide **Installation** section.

    :Note: the ``nibabel`` package is excluded from this install,
        since nibabel validation requires ``numpy`` to be installed
        before any of the dependent packages are installed. ``numpy``
        and ``nibabel`` must be installed separately as described
        in the User Guide **Installation** section. 

    :return: the ``requirements.txt`` PyPI package specifications
    """
    with open('requirements.txt') as f:
        rqmts = f.read().splitlines()
        pypi_rqmts = [rqmt for rqmt in rqmts if not VCS_RQMT_PAT.match(rqmt)]
    # If numpy is not installed, then don't include nibabel.
    try:
        import numpy
        return pypi_rqmts
    catch ImportError:
        warning.warn("numpy must be installed separately prior to qipipe."
                     " This qipipe installation is adequate only for a"
                     " ReadTheDocs build.")
        return [rqmt for rqmt in pypi_rqmts if not rqmt.startswith('nibabel')]


def dependency_links():
    """
    Returns the non-PyPI ``qipipe`` requirements in
    ``requirements.txt`` which match the :const:`VCS_RQMT_PAT`
    pattern. See the :meth:`requires` note.

    :Note: the ``dcmstack`` package is excluded from this install,
        since it depends on nibabel which is excluded from :meth:`requires`.
    """
    with open('requirements.txt') as f:
        rqmts = f.read().splitlines()
        ext_rqmts = [rqmt for rqmt in rqmts if VCS_RQMT_PAT.match(rqmt)]
    # If numpy is not installed, then don't include dcmstack.
    try:
        import numpy
        return ext_rqmts
    catch ImportError:
        warning.warn("numpy must be installed separately prior to qipipe."
                     " This qipipe installation is adequate only for a"
                     " ReadTheDocs build.")
        return [rqmt for rqmt in ext_rqmts if not rqmt.endswith('dcmstack')]


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
