import os
import qipipe

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx', 'sphinx.ext.todo']
autoclass_content = "both"
source_suffix = '.rst'
master_doc = 'index'
project = u'qipipe'
copyright = u'2014, OHSU Knight Cancer Institute'
version = qipipe.__version__
pygments_style = 'sphinx'
htmlhelp_basename = 'qipipedoc'
html_title = "qipipe v%s" % version
