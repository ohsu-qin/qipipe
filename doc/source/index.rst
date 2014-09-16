.. _index:

==========================
qipipe - OHSU QIN pipeline
==========================

********
Synopsis
********
qipipe processes the OHSU QIN study images.

:API: http://quip1.ohsu.edu:8080/qipipe/api

:Git: git\@quip1.ohsu.edu:qipipe
      (`Browse <http://quip1.ohsu.edu:6060/qipipe>`__)


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
1. Install Git_ on your workstation, if necessary.

2. Contact the qipipe `OHSU QIN Git administrator`_ to get permission to
   access the qipipe Git repository.

3. Install qiutil_.

4. Clone the `qipipe repository`_::

       cd ~/workspace
       git clone git@quip1:qipipe
   
5. Install Anaconda_ on your workstation, if necessary.

6. Make an Anaconda virtual environment::

       cd ~/workspace/qipipe
       conda create --name qipipe scipy
   
   The Anaconda ``conda`` command is a pip-like utility that installs packages
   managed by Anaconda. The ``conda create`` step makes a virtual environment
   with one package. ``conda create`` requires at least one package, but fails if
   the package is not managed by Anaconda. Therefore, creating the environment
   with one known package makes the environment.

7. Activate the ``qipipe`` environment::

       source activate qipipe
   
   Sourcing ``activate`` prepends the ``qipipe`` environment bin path to the
   ``PATH`` environment variable.

8. Install packages mmanaged by Anaconda::

       for p in `cat requirements.txt`; do conda install $p; done
   
   The ``for`` loop attempts to install packages managed by Anaconda one at a
   time. Package installation will fail for packages not managed by Anaconda.
   Anaconda installations are preferred because Anaconda attempts to impose
   additional constraints to ensure the consistency of the Python scientific
   platform.

9. Install the ``qipipe`` package::

       pip install -e .
       pip install -r requirements

   The first command installs the non-Git dependencies in ``requirements.txt``
   that were not installed by Anaconda. The second command installs the
   remaining Git dependencies in ``requirements.txt``.


*****
Usage
*****
Run the following command for the pipeline options::

     qipipe --help

The `OHSU QIN Sharepoint`_ `TCIA Upload Procedure`_ document describes how
to import the staged QIN images into TCIA.

---------

.. container:: copyright

  Copyright (C) 2014 Oregon Health & Science University
  `Knight Cancer Institute`_. All rights reserved. ``qipipe`` is confidential
  and may not be distributed in any form without authorization.


.. Targets:

.. _Advanced Imaging Research Center: http://www.ohsu.edu/xd/research/centers-institutes/airc/

.. _Anaconda: http://docs.continuum.io/anaconda/

.. _Git: http://git-scm.com

.. _Knight Cancer Institute: http://www.ohsu.edu/xd/health/services/cancer

.. _OHSU QIN Git administrator: loneyf@ohsu.edu

.. _OHSU QIN Sharepoint: https://bridge.ohsu.edu/research/knight/projects/qin/SitePages/Home.aspx

.. _pip: https://pypi.python.org/pypi/pip

.. _Python: http://www.python.org

.. _QIN XNAT: http://quip5.ohsu.edu:8080/xnat

.. _QIN collection: https://wiki.cancerimagingarchive.net/display/Public/Quantitative+Imaging+Network+Collections

.. _qipipe repository: http://quip1.ohsu.edu:6060/qipipe

.. _qiutil: http://quip1.ohsu.edu:6060/qiutil

.. _TCIA Upload Procedure: https://bridge.ohsu.edu/research/knight/projects/qin/_layouts/WordViewer.aspx?id=/research/knight/projects/qin/Shared%20Documents/TCIA%20upload%20procedure.docx&Source=https%3A%2F%2Fbridge%2Eohsu%2Eedu%2Fresearch%2Fknight%2Fprojects%2Fqin%2FSitePages%2FHome%2Easpx&DefaultItemOpen=1&DefaultItemOpen=1

.. _The Cancer Imaging Archive: http://cancerimagingarchive.net


.. toctree::
  :hidden:

  api/index
  api/helpers
  api/interfaces
  api/pipeline
  api/staging
