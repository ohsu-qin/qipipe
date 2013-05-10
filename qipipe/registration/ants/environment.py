import os
from .ants_error import ANTSError

_environ = None

def ants_environ():
    """
    Makes the minimal environment variable dictionary for executing an ANTS script.
    
    @return: the environment dictionary
    @raise ANTSError: if the ANTSPATH environment variable is not set
    """
    # Lazy initializer.
    global _environ
    if not _environ:
        ants_path = os.getenv('ANTSPATH')
        if not ants_path:
            raise ANTSError("ANTSPATH environment variable is not set.")
        _environ = dict(ANTSPATH=ants_path)
    return _environ
