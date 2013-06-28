"""
The ``pipelines`` module includes the following QIN pipelines:

- :mod:`.qipipeline`: the soup-to-nuts pipeline to stage, register and model new images

- :mod:`.staging`: executes the staging workflow to detect new images, group them by
  series, import them into XNAT and prepare them for TCIA import
  
- :mod:`.registration`: mask the target tissue and corrects motion artifacts

- :mod:`.modeling`: perform pharmokinetic modeling
"""
