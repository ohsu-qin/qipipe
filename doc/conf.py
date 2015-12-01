import os
try:
    import qipipe
except ImportError:
    # Load the module directly.
    src_dir = os.path.join(os.path.dirname(__file__), '..', 'qipipe')
    sys.path.append(src_dir)
    import qipipe

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx', 'sphinx.ext.todo']
autoclass_content = "both"
autodoc_default_flags= ['members', 'show-inheritance']
source_suffix = '.rst'
master_doc = 'index'
project = u'qipipe'
copyright = u'2014, OHSU Knight Cancer Institute'
version = qipipe.__version__
pygments_style = 'sphinx'
htmlhelp_basename = 'qipipedoc'
html_title = "qipipe v%s" % version


def skip(app, what, name, obj, skip, options):
    """
    @return False if the name is __init__ or *skip* is set, True otherwise
    """
    return skip and name is not "__init__"


def setup(app):
    """
    Directs autodoc to call :meth:`skip` to determine whether to skip a member.
    """
    app.connect("autodoc-skip-member", skip)
