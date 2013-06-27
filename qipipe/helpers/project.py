def project(name=None):
    """
    Gets or sets the current XNAT project name.
    If there is a name argument, then the current XNAT project name
    is set to that value and returns the new project name.
    Otherwise, this function returns the current XNAT project name.
    The default project id is ``QIN``.
    
    The ``qipipe`` module does not reset the current project. Testing
    modules can reset the project to override the default in order
    to define a XNAT test project.
    
    :param name: the XNAT project name to make current
    :return: the current XNAT project name
    """
    if name:
        project.name = name
    elif not hasattr(project, 'name'):
        project.name = 'QIN'
    
    return project.name
