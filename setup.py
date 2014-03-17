import glob
from setuptools import (setup, find_packages)


def requires():
    with open('requirements.txt') as f:
        return f.read().splitlines()


def readme():
    with open("README.rst") as f:
        return f.read()


setup(
    name = 'qipipe',
    version = '4.1.2',
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
    install_requires = requires()
)
