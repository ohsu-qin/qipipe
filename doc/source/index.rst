.. _index:

==========================
qipipe - OHSU QIN pipeline
==========================

********
Synopsis
********
qipipe processes the OHSU QIN study images.

:API: http://quip1.ohsu.edu:8080/qipipe/api

:Git: git\@quip1.ohsu.edu:qipipe (`Browse <http://quip1.ohsu.edu:6060/qipipe>`__)

************
Feature List
************
1. Recognition of new AIRC QIN study images.

2. Staging for submission to `The Cancer Imaging Archive`_ (TCIA) `QIN collection`_.

3. Masking to subtract extraneous image content.

4. Motion correction.

5. Pharmokinetic mapping.

6. Import of the input scans and processing results into the OSHU `QIN XNAT`_ instance.

**********
Installing
**********
1. Install Git_ on your workstation.

2. Contact the qipipe `OHSU QIN Git administrator`_ to get permission to access the qipipe Git
   repository.

3. Clone the `qipipe repository`_::

       cd ~/workspace
       git clone git@quip1:qipipe
   
4. Install the Python_ pip_ package on
   your workstation.

5. Install the qipipe package::

       cd ~/workspace/qipipe
       pip install -e .

*****
Usage
*****
Run the following command for the pipeline options::

     qipipeline --help

The `OHSU QIN Sharepoint`_ `TCIA Upload Procedure`_ document describes how to import the staged QIN images into TCIA.

---------

Copyright (C) 2013, Oregon Health & Science University `Knight Cancer Institute`_. All rights reserved.
qipipe is confidential and may not be distributed in any form without authorization.


.. Targets:

.. _Advanced Imaging Research Center: http://www.ohsu.edu/xd/research/centers-institutes/airc/

.. _Git: http://git-scm.com

.. _Knight Cancer Institute: http://www.ohsu.edu/xd/health/services/cancer

.. _OHSU QIN Git administrator: loneyf@ohsu.edu

.. _OHSU QIN Sharepoint: https://bridge.ohsu.edu/research/knight/projects/qin/SitePages/Home.aspx

.. _pip: https://pypi.python.org/pypi/pip

.. _Python: http://www.python.org

.. _QIN XNAT: http://quip5.ohsu.edu:8080/xnat

.. _QIN collection: https://wiki.cancerimagingarchive.net/display/Public/Quantitative+Imaging+Network+Collections

.. _qipipe repository: http://quip1.ohsu.edu:6060/qipipe

.. _TCIA Upload Procedure: https://bridge.ohsu.edu/research/knight/projects/qin/_layouts/WordViewer.aspx?id=/research/knight/projects/qin/Shared%20Documents/TCIA%20upload%20procedure.docx&Source=https%3A%2F%2Fbridge%2Eohsu%2Eedu%2Fresearch%2Fknight%2Fprojects%2Fqin%2FSitePages%2FHome%2Easpx&DefaultItemOpen=1&DefaultItemOpen=1

.. _The Cancer Imaging Archive: http://cancerimagingarchive.net


.. toctree::
  :hidden:

  api/index
  api/helpers
  api/interfaces
  api/pipelines
  api/staging
