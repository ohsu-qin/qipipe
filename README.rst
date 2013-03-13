qipipe: Quantitative Imaging Pipeline
=====================================
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

* `staging` : prepare the image files

* `registration` : Image registration

* `xnat`: Import into the OHSU XNAT instance

Installing
----------
1. Install [git](http://git-scm.com) on your workstation.

1. Clone this qipipe repository:

       cd ~/workspace
       git clone git@quip1:qipipe
   
2. Install the [Python](http://www.python.org) [setuptools](http://pypi.python.org/pypi/setuptools) package on
   your workstation.

3. Install the qipipe package:

       cd ~/workspace/qipipe
       easy_install .

Usage
-----
The OHSU QIN Sharepoint
[TCIA Upload Procedure](https://bridge.ohsu.edu/research/knight/projects/qin/_layouts/WordViewer.aspx?id=/research/knight/projects/qin/Shared%20Documents/TCIA%20upload%20procedure.docx&Source=https%3A%2F%2Fbridge%2Eohsu%2Eedu%2Fresearch%2Fknight%2Fprojects%2Fqin%2FSitePages%2FHome%2Easpx&DefaultItemOpen=1&DefaultItemOpen=1)
document describes how to import new QIN images into TCIA.

Copyright
---------
qipipe &copy; 2012 by [Oregon Health & Science University](http://www.ohsu.edu/xd/health/services/cancer).
qipipe is confidential and may not be distributed in any form without authorization.
