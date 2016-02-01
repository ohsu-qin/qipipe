"""
This ``pipeline`` module includes the following workflows:

- :mod:`qipipe.pipeline.qipipeline`: the soup-to-nuts pipeline to stage, mask,
  register and model new images

- :mod:`qipipe.pipeline.staging`: executes the staging workflow to detect new
  images, group them by volume, import them into XNAT and prepare them for
  TCIA import

- :mod:`qipipe.pipeline.mask`: creates a mask to subtract extraneous tissue
  from the input images

- :mod:`qipipe.pipeline.registration`: masks the target tissue and corrects
  motion artifacts

- :mod:`qipipe.pipeline.modeling`: performs pharmokinetic modeling
"""
