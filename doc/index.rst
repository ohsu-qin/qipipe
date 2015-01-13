.. _index:

==========================
qipipe - OHSU QIN pipeline
==========================

********
Synopsis
********
qipipe processes the OHSU QIN study images.

:API: http://qipipe.readthedocs.org/en/latest/

:Git: https://github.com/ohsu-qin/qipipe


************
Feature List
************
1. Recognizes new AIRC QIN study images.

2. Stages images for submission to 
   `The Cancer Imaging Archive`_ (TCIA) `QIN collection`_.

3. Masks images to subtract extraneous image content.

4. Corrects motion artifacts.

5. Performs pharmokinetic modeling.

6. Imports the input scans and processing results into the OSHU `QIN XNAT`_
   instance.


************
Installation
************
The following instructions assume that you start in your home directory.
``qipipe`` has dependencies with special installation requirements.
Consequently, ``qipipe`` installation cannot be performed using the
customary Python_ pip_ command ``pip install qixnat`` alone. Install
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

4. Refresh your environment, e.g. quit your console and reopen a new one.
   
5. Install ``qixnat`` using Anaconda_ as described in the
   `qixnat installation instructions`_.

6. Install the ``qipipe`` dependencies hosted by Anaconda::

      wget -O - https://raw.githubusercontent.com/ohsu-qin/qipipe/master/requirements.txt | xargs -n 1 conda install -y

  Ignore ``No packages found`` messages for non-Anaconda packages. These
  packages will be installed in the next step.

7. Install the ``qipipe`` dependencies hosted by pip::

      wget -O - https://raw.githubusercontent.com/ohsu-qin/qipipe/master/requirements.txt | xargs -n 1 pip install

  The dependencies must be installed in succession one at a time because some requirements,
  e.g. ``nipy``, have implicit dependencies that necessitate this one-at-a-time approach.

8. Install the ``qipipe`` package::

       pip install qipipe

9. Install any additional packages used in the pipeline. For example, the base
   installation has an optional pipeline modeling phase which uses the OHSU
   proprietary shutter speed PK modeling package.


*****
Usage
*****
Run the following command for the pipeline options::

     qipipe --help

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

.. _pip: https://pypi.python.org/pypi/pip

.. _pip Installation Instructions: http://pip.readthedocs.org/en/latest/installing.html

.. _Python: http://www.python.org

.. _QIN XNAT: http://quip5.ohsu.edu:8080/xnat

.. _qixnat: https://github.com/ohsu-qin/qixnat

.. _qixnat installation instructions: https://github.com/ohsu-qin/qixnat/blob/master/doc/index.rst

.. _QIN collection: https://wiki.cancerimagingarchive.net/display/Public/Quantitative+Imaging+Network+Collections

.. _qipipe repository: https://github.com/ohsu-qin/qipipe

.. _qixnat: https://github.com/ohsu-qin/qixnat

.. _The Cancer Imaging Archive: http://cancerimagingarchive.net


.. toctree::
  :hidden:

  api/index
  api/helpers
  api/interfaces
  api/pipeline
  api/staging
