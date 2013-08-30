import os


def which(program):
    """
    Returns whether the given program is executable.
    This function is a system-independent Python equivalent of the \*nix
    ``which`` command.
    
    The implementation is stolen from
    http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python.
    
    :param program: the program to check
    :return: whether the given program is an exectuable file
        in the system path
    """
    fpath, fname = os.path.split(program)
    if fpath:
        if is_executable(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_executable(exe_file):
                return exe_file

    return None


def is_executable(fpath):
    """
    :param fpath: the file path to check
    :return: whether the file exists and is executable
    """
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
