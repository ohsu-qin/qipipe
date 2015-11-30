.. _index:

======================================
qipipe - Quantitative Imaging pipeline
======================================

********
Synopsis
********
qipipe processes DICOM study images.

:API: http://qipipe.readthedocs.org/en/latest/api/index.html

:Git: https://github.com/ohsu-qin/qipipe


************
Feature List
************
1. Recognizes new study images.

2. Stages images for submission to 
   `The Cancer Imaging Archive`_ (TCIA) `QIN collection`_.

3. Masks images to subtract extraneous image content.

4. Corrects motion artifacts.

5. Performs pharmokinetic modeling.

6. Imports the input scans and processing results into the XNAT_
   image database.


************
Installation
************
The following instructions assume that you start in your home directory.
``qipipe`` has dependencies with special installation requirements.
Consequently, ``qipipe`` installation cannot be performed using the
customary Python_ pip_ command ``pip install qipipe`` alone. Install
``qipipe`` using the following procedure:

1. Install Git_ on your workstation, if necessary.

2. Build ANTS_ from source using the `ANTS Compile Instructions`_::

       pushd ~/workspace
       git clone git://github.com/stnava/ANTs.git
       mkdir $HOME/ants
       cd $HOME/ants
       ccmake ../workspace/ANTs
       cmake
       #=> Enter “c"
       #=> Enter “g”
       #=> Exit back to the terminal
       make -j 4
       popd

3. Prepend ANTS to your shell login script. Open an editor on
   ``$HOME/.bashrc`` or ``$HOME/.bash_profile`` and add the following
   lines::

       # Prepend ANTS to the path.
       ANTS_HOME=$HOME/ants
       export PATH=$ANTS_HOME/bin

4. Refresh your environment::

       . $HOME/.bash_profile
   
5. Install and activate an Anaconda_ environment as described in the
   `qixnat installation instructions`_.

6. Install the ``qipipe`` dependencies hosted by Anaconda::

       wget -q --no-check-certificate -O \
         - https://www.github.com/ohsu-qin/qipipe/raw/master/requirements_conda.txt \
         | xargs conda install --yes

7. Install the ``nibabel`` package separately::

       pip install nibabel

   This package must be installed separately to avoid a ``nipype`` installation error. 

8. Install the ``qipipe`` package::

       pip install qipipe


*****
Usage
*****
Run the following command for the pipeline options::

    qipipe --help


***********
Development
***********

Download
--------
Download the source by cloning the `source repository`_, e.g.::

    cd ~/workspace
    git clone https://github.com/ohsu-qin/qipipe.git
    cd qipipe

Installing from a local qipipe clone requires the constraints option::

    pip install -c constraints.txt -e .

Testing
-------
Testing is performed with the nose_ package, which can be installed
separately as follows::

    conda install nose

The unit tests are then run as follows::

    nosetests test/unit/

Documentation
-------------
API documentation is built automatically by ReadTheDocs_ when the
project is pushed to GitHub. The modules documented are defined in
``doc/api``. If you add a new Python file to a package directory
*pkg*, then include it in the ``doc/api``\ *pkg* \``.rst`` file.

Documentation can be generated locally as follows:

* Install Sphinx_ and ``docutils``, if necessary::

      conda install Sphinx docutils

* Run the following in the ``doc`` subdirectory::

      make html

Release
-------
Building a release requires a PyPI_ account and the twine_ package,
which can be installed separately as follows::

    pip install twine

The release is then published as follows:

* Confirm that the unit tests run without failure.

* Add a one-line summary release theme to ``History.rst``.

* Update the ``__init__.py`` version.

* Commit these changes::

      git add --message 'Bump version.' History.rst qipipe/__init__.py

* Merge changes on a branch to the current master branch, e.g.::

      git checkout master
      git pull
      git merge --no-ff <branch>

* Reconfirm that the unit tests run without failure.

* Build the release::

      python setup.py sdist

* Upload the release::

      twine upload dist/qipipe-<version>.tar.gz

* Tag the release::

      git tag v<n.n.n>

* Update the remote master branch::

      git push
      git push --tags

---------

.. container:: copyright

  Copyright (C) 2014 Oregon Health & Science University
  `Knight Cancer Institute`_. All rights reserved.
  See the license_ for permissions.


.. Targets:

.. _Anaconda: http://docs.continuum.io/anaconda/

.. _Anaconda Installation Instructions: http://docs.continuum.io/anaconda/install.html

.. _ANTS: http://stnava.github.io/ANTs/

.. _ANTS Compile Instructions: http://brianavants.wordpress.com/2012/04/13/updated-ants-compile-instructions-april-12-2012/

.. _Git: http://git-scm.com

.. _Knight Cancer Institute: http://www.ohsu.edu/xd/health/services/cancer

.. _license: https://github.com/ohsu-qin/qipipe/blob/master/LICENSE.txt

.. _nose: https://nose.readthedocs.org/en/latest/

.. _pip: https://pypi.python.org/pypi/pip

.. _pip Installation Instructions: http://pip.readthedocs.org/en/latest/installing.html

.. _PyPI: https://pypi.python.org/pypi

.. _Python: http://www.python.org

.. _qixnat: https://github.com/ohsu-qin/qixnat

.. _qixnat installation instructions: https://github.com/ohsu-qin/qixnat/blob/master/doc/index.rst

.. _QIN collection: https://wiki.cancerimagingarchive.net/display/Public/Quantitative+Imaging+Network+Collections

.. _source repository: https://github.com/ohsu-qin/qipipe

.. _qixnat: https://github.com/ohsu-qin/qixnat

.. _ReadTheDocs: https://www.readthedocs.org

.. _Sphinx: http://sphinx-doc.org/index.html

.. _The Cancer Imaging Archive: http://cancerimagingarchive.net

.. _twine: https://pypi.python.org/pypi/twine

.. _XNAT: http://www.xnat.org/


.. toctree::
  :hidden:

  api/index
  api/helpers
  api/interfaces
  api/pipeline
  api/staging
