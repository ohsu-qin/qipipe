import os
import re
import glob
from setuptools import (setup, find_packages)


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
    
    :Note: ``pip`` supports GitHub package specifications,
       but a local ``pip install -e .`` does not. Therefore,
       the requirements must be installed as described in
       the User Guide **Installation** section. This method
       filters out the GitHub portion of the specification
       and expects these dependencies to already be installed
       before package setup.
    """
    with open('requirements.txt') as f:
        return f.read().splitlines()


def readme():
    with open("README.rst") as f:
        return f.read()


setup(
    name = 'qipipe',
    version = version('qipipe'),
    author = 'OHSU Knight Cancer Institute',
    author_email = 'loneyf@ohsu.edu',
    packages = find_packages(),
    data_files=[('config', glob.glob('conf/*.cfg'))],
    scripts = glob.glob('bin/*'),
    url = 'http://quip1.ohsu.edu/8080/qipipe',
    description = 'qipipe processes the OHSU QIN images.',
    long_description = readme(),
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    install_requires = requires(),
    dependency_links = [
        "git+http://github.com/moloney/dcmstack.git@0.7.dev#egg=dcmstack"
        "git+http://github.com/FredLoney/nipype.git#egg=nipype-master"
        "git+http://github.com/FredLoney/pyxnat.git#egg=pyxnat"
    ]
)
