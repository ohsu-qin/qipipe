import glob
from distutils.core import setup


requires = ['pydicom']

setup(
    name = 'qipipe',
    version = '1.1.1',
    author = 'Fred Loney',
    author_email = 'loneyf@ohsu.edu',
    packages = find_packages('lib'),
    package_dir = {'':'lib'}
    scripts = glob.glob('bin/*'),
    url = 'http://quip1.ohsu.edu/git/qipipe',
    license = 'Proprietary',
    description = '.',
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
