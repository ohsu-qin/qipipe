import os
import qipipe

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx', 'sphinx.ext.todo']
autoclass_content = "both"
templates_path = ['templates']
source_suffix = '.rst'
master_doc = 'index'
project = u'qipipe'
copyright = u'2014, OHSU Knight Cancer Institute'
version = qipipe.__version__
pygments_style = 'sphinx'
html_theme = 'qipipe_theme'
html_theme_path = ['.']
html_theme_options = dict(linkcolor='DarkSkyBlue', visitedlinkcolor='Navy')
htmlhelp_basename = 'qipipedoc'
html_title = "qipipe v%s" % version
