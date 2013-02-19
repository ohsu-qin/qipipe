from nipype.interfaces.utility import Function

def _compress(in_file, dest=None):
    """
    Compresses the given file.
    
    @param in_file: the path of the file to compress
    @parma dest: the destination (default is the working directory)
    @return: the compressed file path
    """
    import os, gzip
    
    if not dest:
        dest = os.getcwd()
    if not os.path.exists(dest):
        os.makedirs(dest)
    _, fname = os.path.split(in_file)
    out_file = os.path.join(dest, fname + '.gz')
    f = open(in_file, 'rb')
    cf = gzip.open(out_file, 'wb')
    cf.writelines(f)
    f.close()
    cf.close()
    return os.path.abspath(out_file)

compress = Function(input_names=['in_file', 'dest'], output_names=['out_file'], function=_compress)
"""The file compressor function."""

def _uncompress(in_file, dest=None):
    """
    Uncompresses the given file.
    
    @param in_file: the path of the file to uncompress
    @parma dest: the destination (default is the working directory)
    @return: the compressed file path
    """
    import os, gzip
    
    if not dest:
        dest = os.getcwd()
    if not os.path.exists(dest):
        os.makedirs(dest)
    _, fname = os.path.split(in_file)
    out_file = os.path.join(dest, fname[:-3])
    cf = gzip.open(in_file, 'rb')
    f = open(out_file, 'wb')
    f.writelines(cf)
    f.close()
    cf.close()
    return os.path.abspath(out_file)

uncompress = Function(input_names=['in_file', 'dest'], output_names=['out_file'], function=_uncompress)
"""The file uncompressor function."""

def _flatten(in_list):
    """
    @param in_list: the nested list to flatten
    @return: the flattened list
    """
    from itertools import chain
    
    return list(chain.from_iterable(in_list))

flatten = Function(input_names=['in_list'], output_names=['out_list'], function=_flatten)
"""The list flattener function."""

PVS_KEYS = ['_p', '_v', '_s']
"""The patient/visit/series substitution keys."""

def _pvs_substitution(path=None):
    """
    Builds a nipype DataSink patient/visit/series substitution list from the given
    series directory path. The substitution list associates keys to the right-most
    path components, e.g.:
    
    >>> _dir_substitution('/path/to/patient03/visit04/series002')
    [('_p', 'patient03'), ('_v', 'visit04'), ('_s', 'series0002')]
    
    @param path: the file path
    @return: the substitution list
    """
    import os
    # Redefine the global to work around nipype limitation. nipype does not implement
    # Function closures, so the full context must be defined within the function.
    PVS_KEYS = ['_p', '_v', '_s']
    
    if not path:
        return None
    components = path.split(os.path.sep)
    return [(PVS_KEYS[i], components[i-len(PVS_KEYS)]) for i in xrange(len(PVS_KEYS))]

pvs_substitution = Function(input_names=['path'], output_names=['substitutions'], function=_pvs_substitution)
"""The patient/visit/series substitution function."""
