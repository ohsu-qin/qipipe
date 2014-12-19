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
The following instructions assume that you start in your home directory.
Note that Python installation environments are usually fragile and these
instructions can break in unanticipated ways. Caveat emptor.

1. Install Git_ on your workstation, if necessary.

2. Contact the qipipe `OHSU QIN Git administrator`_ to get permission to
   access the qipipe Git repository.

3. Build ANTS_ from source using the `ANTS Compile Instructions`_::

       pushd ~/workspace
       git clone git://github.com/stnava/ANTs.git
       mkdir ~/ants
       cd ~/ants
       ccmake ..workspace/ANTs
       cmake
       #=> Enter “c"
       #=> Enter “g”
       #=> Exit back to the terminal
       make -j 4
       popd
   
4. Install Anaconda_ in ``~/anaconda`` on your workstation according to
   the `Anaconda Installation Instructions`_.

5. Make an Anaconda virtual environment::

       conda create --name qipipe scipy

   The Anaconda ``conda`` command is a pip-like utility that installs packages
   managed by Anaconda. The ``conda create`` step makes a virtual environment
   with one package. ``conda create`` requires at least one package, but fails
   if the package is not managed by Anaconda. Therefore, creating the environment
   with one known package makes the environment.

6. Prepend Git, ANTS, Anaconda and your virtual environment to ``PATH`` in your shell
   login script. Open an editor on ~/.bashrc or ~/.bash_profile and add the
   following lines::

      # Prepend the locally installed applications.
      export PATH=/path/to/git/bin:$ANTS_HOME/bin:$ANACONDA_HOME:$PATH
      # Prepend the qipipe virtual environment.
      . $HOME/anaconda/bin/activate qipipe      

7. Refresh your environment, e.g. quit your ssh session and reopen a new one.

8. Install qiutil_.

9. Clone the `qipipe repository`_::

       cd ~/workspace
       git clone git@quip1:qipipe
       cd qipipe

10. Install packages managed by Anaconda::

       for p in `cat requirements.txt`; do conda install $p; done
   
   The ``for`` loop attempts to install packages managed by Anaconda one at a
   time. Package installation will fail for packages not managed by Anaconda.
   Anaconda installations are preferred because Anaconda attempts to impose
   additional constraints to ensure the consistency of the Python scientific
   platform.

11. Install the remaining dependency packages using pip_::

       for p in `cat requirements.txt`; do pip install $p; done

    Use this command in preference to ``pip install -r requirements.txt``
    in order to install Git repository packages.

12. On Linux only, install mpi4py::
       
       pip install mpi4py=1.3.1

13. Install the ``qipipe`` package::

       pip install -e .

14. If you will be running the PK modeling workflow, then install the proprietary
    ``fastfit`` module from the Mercurial repository::
    
       pip install hg+https://everett.ohsu.edu/hg/fastfit#egg=fastfit


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

.. _Anaconda Installation Instructions: http://docs.continuum.io/anaconda/install.html

.. _ANTS: http://stnava.github.io/ANTs/

.. _ANTS Compile Instructions: http://brianavants.wordpress.com/2012/04/13/updated-ants-compile-instructions-april-12-2012/

.. _Git: http://git-scm.com

.. _Knight Cancer Institute: http://www.ohsu.edu/xd/health/services/cancer

.. _OHSU QIN Git administrator: loneyf@ohsu.edu

.. _OHSU QIN Sharepoint: https://bridge.ohsu.edu/research/knight/projects/qin/SitePages/Home.aspx

.. _pip: https://pypi.python.org/pypi/pip

.. _pip Installation Instructions: http://pip.readthedocs.org/en/latest/installing.html

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
