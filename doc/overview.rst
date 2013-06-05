qipipe: OHSU Quantitative Imaging Pipeline
==========================================
**Home**:         [https://bridge.ohsu.edu/research/knight/projects/qin/SitePages/Home.aspx](https://bridge.ohsu.edu/research/knight/projects/qin/SitePages/Home.aspx)    
**Git**:          [http://quip1.ohsu.edu/git/qipipe](http://quip1.ohsu.edu/git/qipipe)       
**Author**:       OHSU Knight Cancer Institute    
**Copyright**:    2012    
**License**:      Proprietary    

Synopsis
--------
qipipe processes the OHSU [QIN](https://bridge.ohsu.edu/research/knight/projects/qin/SitePages/Home.aspx) study images.

Feature List
------------
1. Image import into the OSHU QIN [XNAT](http://quip1.ohsu.edu/) instance.

1. Motion correction.

2. Submission to [The Cancer Imaging Archive](http://cancerimagingarchive.net) (TCIA)
[QIN collection](https://wiki.cancerimagingarchive.net/display/Public/Quantitative+Imaging+Network+Collections).

Content
-------
The main qipipe modules include the following:

* `helpers` : Common utilities

* `interfaces` : Nipype workflow interfaces

* `pipelines` : Image staging, registration and PK mapping workflow 

* `staging` : Prepare the image files

Installing
----------
1. Install [Git](http://git-scm.com) on your workstation.

2. Contact the qipipe [Git administrator](mail:loneyf@ohsu.edu) to get permission to access the qipipe Git
   repository.

3. Clone this qipipe repository::

       cd ~/workspace
       git clone git@quip1:qipipe
   
4. Install the [Python](http://www.python.org) [pip](https://pypi.python.org/pypi/pip) package on
   your workstation.

5. Install the qipipe package::

       cd ~/workspace/qipipe
       pip install -e .

Usage
-----
Run the following command for the pipeline options::

     qipipeline --help

The OHSU QIN Sharepoint
[TCIA Upload Procedure](https://bridge.ohsu.edu/research/knight/projects/qin/_layouts/WordViewer.aspx?id=/research/knight/projects/qin/Shared%20Documents/TCIA%20upload%20procedure.docx&Source=https%3A%2F%2Fbridge%2Eohsu%2Eedu%2Fresearch%2Fknight%2Fprojects%2Fqin%2FSitePages%2FHome%2Easpx&DefaultItemOpen=1&DefaultItemOpen=1)
document describes how to import new QIN images into TCIA.

Copyright
---------
qipipe &copy; 2012 by [Oregon Health & Science University](http://www.ohsu.edu/xd/health/services/cancer).
qipipe is confidential and may not be distributed in any form without authorization.
