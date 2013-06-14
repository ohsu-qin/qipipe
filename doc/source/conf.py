import qipipe

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx', 'sphinx.ext.todo']
autoclass_content = "both"
templates_path = ['templates']
source_suffix = '.rst'
master_doc = 'index'
project = u'qipipe'
copyright = u'2013, OHSU Knight Cancer Institute'
version = qipipe.__version__
pygments_style = 'sphinx'
html_theme = 'qipipe_theme'
html_theme_path = ['.']
htmlhelp_basename = 'qipipedoc'
html_title = "qipipe v%s" % version


def skip(app, what, name, obj, skip, options):
    """Document the __init__ methods."""
    if name == "__init__":
        return False
    return skip

def setup(app):
    app.connect("autodoc-skip-member", skip)