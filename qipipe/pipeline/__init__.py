"""
The ``pipeline`` module includes the following QIN pipeline:

- :mod:`.qipipeline`: the soup-to-nuts pipeline to stage, register and model new images

- :mod:`.staging`: executes the staging workflow to detect new images, group them by
  series, import them into XNAT and prepare them for TCIA import
  
- :mod:`.registration`: masks the target tissue and corrects motion artifacts

- :mod:`.modeling`: performs pharmokinetic modeling
"""
