import glob
from setuptools import setup, find_packages

__version__ = open('version.txt').read()

__doc__ = 'qipipe processes the OHSU QIN images. See the README file for more information.'

requires = ['pydicom', 'envoy']

setup(
    name = 'qipipe',
    version = __version__,
    author = 'Fred Loney',
    author_email = 'loneyf@ohsu.edu',
    packages = find_packages(),
    scripts = glob.glob('bin/*'),
    url = 'http://quip1.ohsu.edu/git/qipipe',
    license = 'Proprietary',
    description = __doc__.split('.', 1)[0],
    long_description = __doc__,
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
    ],
    install_requires = requires
)
