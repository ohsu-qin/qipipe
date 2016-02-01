import os
import re
import tempfile
import pprint
import networkx as nx
import qixnat
from qiutil.logging import logger
from qiutil.collections import EMPTY_DICT
from qiutil.ast_config import read_config
from ..helpers.constants import CONF_DIR
from ..helpers.distributable import DISTRIBUTABLE
from .pipeline_error import PipelineError


class WorkflowBase(object):
    """
    The WorkflowBase class is the base class for the qipipe workflow
    wrapper classes.
    
    If the :attr:`distributable' flag is set, then the execution is
    distributed using the Nipype plug-in specified in the configuration
    *plug_in* parameter.
    
    The workflow plug-in arguments and node inputs can be specified in
    a :class:`qiutil.ast_config.ASTConfig` file. The configuration
    directory order consist of the order consist of the search locations
    in low-to-high precedence order consist of the following:
    
    1. the qipipe module ``conf`` directory
    
    2. the *config_dir* initialization keyword option
    
    The common configuration is loaded from the ``default.cfg`` file or
    in the directory locations. The workflow-specific configuration file
    name is the lower-case name of the ``WorkflowBase`` subclass with
    ``.cfg`` extension, e.g. ``registration.cfg`` for
    :class:`qipipe.workflow.registration.RegistrationWorkflow`.
    The configuration settings are then loaded from the common configuration
    files followed by the workflow-specific configuration files.
    """
    
    CLASS_NAME_PAT = re.compile("^(\w+)Workflow$")
    """The workflow wrapper class name matcher."""
    
    INTERFACE_PREFIX_PAT = re.compile('(\w+\.)+interfaces?\.?')
    """
    Regexp matcher for an interface module.
    
    Example:
    
    >>> from qipipe.pipeline.workflow_base import WorkflowBase
    >>> WorkflowBase.INTERFACE_PREFIX_PAT.match('nipype.interfaces.ants.util.AverageImages').groups()
    ('nipype.',)
    """
    
    MODULE_PREFIX_PAT = re.compile('^((\w+\.)*)(\w+\.)(\w+)$')
    """
    Regexp matcher for a module prefix.
    
    Example:
    
    >>> from qipipe.pipeline.workflow_base import WorkflowBase
    >>> WorkflowBase.MODULE_PREFIX_PAT.match('ants.util.AverageImages').groups()
    ('ants.', 'ants.', 'util.', 'AverageImages')
    >>> WorkflowBase.MODULE_PREFIX_PAT.match('AverageImages')
    None
    """
    
    def __init__(self, **opts):
        """
        Initializes this workflow wrapper object.
        
        :param opts: the following keyword arguments:
        :keyword project: the :attr:`project`
        :keyword logger: the :attr:`logger` to use
        :keyword parent: the parent workflow for a child workflow
        :keyword base_dir: the :attr:`base_dir`
        :keyword config: the optional workflow node :attr:`configuration`
            file location or dictionary
        :keyword dry_run: the :attr:`dry_run` flag
        :keyword distributable: the :attr:`distributable` flag
        :raise PipelineError: if there is neither a *project* nor
            a *parent* argument
        """
        parent = opts.get('parent')
        project_opt = opts.get('project')
        if project_opt:
            project = project_opt
        elif parent:
            project = parent.project
        else:
            raise PipelineError('The workflow wrapper must have either a'
                                ' project or a parent')
        self.project = project
        """The XNAT project name."""
        
        logger_opt = opts.get('logger')
        if logger_opt:
            _logger = logger_opt
        elif parent:
            _logger = parent.logger
        else:
            _logger = logger('qiprofile')
        self.logger = _logger
        """This workflow's logger."""
        
        base_dir_opt = opts.get('base_dir')
        if base_dir_opt:
            base_dir = base_dir_opt
        elif parent:
            base_dir = parent.base_dir
        else:
            base_dir = tempfile.mkdtemp(prefix='qipipe_')
        self.base_dir = base_dir
        "The workflow execution directory (default a new temp directory)."
        
        cfg_dir_opt = opts.get('config_dir')
        if cfg_dir_opt:
            cfg_dir = cfg_dir_opt
        elif parent:
            cfg_dir = parent.config_dir
        else:
            cfg_dir = None
        self.config_dir = cfg_dir
        """The workflow node inputs configuration directory."""
        
        self.configuration = self._load_configuration()
        """The workflow node inputs configuration."""
        
        config_s = pprint.pformat(self.configuration)
        self.logger.debug("Pipeline configuration:")
        for line in config_s.split("\n"):
            self.logger.debug(line)
        
        # The distributable option is only set if the qipipe command
        # option --no-submit is set. In that case, distributable is
        # set to False. Otherwise, the distributable option is not
        # set. Thus, the is_distributable attribute is set to False if
        # either the --no-submit command option was set or there is
        # no cluster environment. Otherwise, there is a cluster
        # environment and the is_distributable flag is set to True.
        distributable_opt = opts.get('distributable')
        if distributable_opt:
            distributable = distributable_opt
        elif parent:
            distributable = parent.is_distributable
        else:
            distributable = DISTRIBUTABLE
        self.is_distributable = distributable
        """Flag indicating whether to submit jobs to a cluster."""
        
        # The execution plug-in.
        if 'Execution' in self.configuration:
            exec_opts = self.configuration['Execution']
            self.plug_in = exec_opts.pop('plug-in', None)
        else:
            self.plug_in = None
        
        if 'dry_run' in opts:
            dry_run = opts.get('dry_run')
        elif parent:
            dry_run = parent.dry_run
        else:
            dry_run = False
        self.dry_run = dry_run
        """Flag indicating whether to prepare but not run the workflow."""
    
    def depict_workflow(self, workflow):
        """
        Diagrams the given workflow graph. The diagram is written to the
        *name*\ ``.dot.png`` in the workflow base directory.
        
        :param workflow the workflow to diagram
        """
        base = workflow.name + '.dot'
        fname = os.path.join(self.base_dir, base)
        workflow.write_graph(dotfilename=fname)
        self.logger.debug("The %s workflow graph is depicted at %s.png." %
                         (workflow.name, fname))
    
    def _load_configuration(self):
        """
        Loads the workflow configuration, as described in
        :class:`WorkflowBase`.
        
        :return: the configuration dictionary
        """
        # The configuration files to load.
        cfg_files = []
        # Validate that the workflow name ends in Workflow.
        match = WorkflowBase.CLASS_NAME_PAT.match(self.__class__.__name__)
        if not match:
            raise NameError("The workflow wrapper class does not match the"
                            " standard workflow class name pattern: %s" %
                            self.__class__.__name__)
        name = match.group(1)
        
        # The default configuration files.
        def_cfg_files = self._configuration_files('default')
        # The workflow configuration files.
        wf_cfg_files = self._configuration_files(name)
        # All path configuration files.
        cfg_files = def_cfg_files + wf_cfg_files
        
        # Load the configuration.
        self.logger.debug("Loading the %s configuration files %s..." %
                          (name, cfg_files))
        cfg = read_config(*cfg_files)
        
        return dict(cfg)
    
    def _configuration_files(self, name):
        """
        :param name: the case-insensitive configuration file name
            without path or extension
        :return: the list of matching configuration files in the configuration
            path, in low-to-high precedence order
        """
        
        # The file name is lower-case with .cfg extension.
        fname = name.lower() + '.cfg'
        # The configuration directories.
        cfg_dirs = [CONF_DIR]
        if self.config_dir:
            cfg_dirs.append(self.config_dir)
        # The configuration file locations.
        cfg_files = []
        for cfg_dir in cfg_dirs:
            cfg_file = os.path.join(cfg_dir, fname)
            if os.path.exists(cfg_file):
                cfg_files.append(os.path.abspath(cfg_file))
        
        return cfg_files
    
    
    def _download_scans(self, xnat, subject, session, dest):
        """
        Download the NIFTI scan files for the given session.
        
        :param xnat: the :class:`qixnat.facade.XNAT` connection
        :param subject: the XNAT subject label
        :param session: the XNAT session label
        :param dest: the destination directory path
        :return: the download file paths
        """
        return xnat.download(self.project, subject, session, dest=dest)
    
    def _run_workflow(self, workflow):
        """
        Executes the given workflow.
        
        :param workflow: the workflow to run
        """
        # If the workflow can be distributed, then get the plugin
        # arguments.
        is_dist_clause = 'is' if self.is_distributable else 'is not'
        self.logger.debug("The %s workflow %s distributable in a"
                           " cluster environment." %
                           (workflow.name, is_dist_clause))
        if self.is_distributable:
            opts = self._configure_plugin(workflow)
        else:
            opts = {}
        
        # Set the base directory to an absolute path.
        if workflow.base_dir:
            workflow.base_dir = os.path.abspath(workflow.base_dir)
        else:
            workflow.base_dir = self.base_dir
        
        # Run the workflow.
        self.logger.debug("Executing the %s workflow in %s..." %
                           (workflow.name, workflow.base_dir))
        if self.dry_run:
            self.logger.debug("Skipped workflow %s job submission,"
                               " since the dry run flag is set." %
                               workflow.name)
        else:
            with qixnat.connect(cachedir=workflow.base_dir):
                workflow.run(**opts)
    
    def _inspect_workflow_inputs(self, workflow):
        """
        Collects the given workflow nodes' inputs for debugging.
        
        :return: a {node name: parameters} dictionary, where *parameters*
            is a node parameter {name: value} dictionary
        """
        return {node_name: self._inspect_node_inputs(workflow.get_node(node_name))
                for node_name in workflow.list_node_names()}
    
    def _inspect_node_inputs(self, node):
        """
        Collects the given node inputs and plugin arguments for debugging.
        
        :return: the node parameter {name: value} dictionary
        """
        fields = node.inputs.copyable_trait_names()
        param_dict = {}
        for field in fields:
            value = getattr(node.inputs, field)
            if value != None:
                param_dict[field] = value
        
        return param_dict
    
    def _configure_plugin(self, workflow):
        """
        Sets the *execution* and plug-in parameters for the given workflow.
        See the ``conf`` directory files for examples.
        
        :param workflow: the workflow to run
        :return: the workflow execution arguments
        """
        # The execution setting.
        if 'Execution' in self.configuration:
            workflow.config['execution'] = self.configuration['Execution']
            self.logger.debug("Workflow %s execution parameters: %s." %
                             (workflow.name, workflow.config['execution']))
        
        # The Nipype plug-in parameters.
        if self.plug_in and self.plug_in in self.configuration:
            plug_in_opts = self.configuration[self.plug_in]
            opts = dict(plugin=self.plug_in, **plug_in_opts)
            self.logger.debug("Workflow %s %s plug-in parameters: %s." %
                             (workflow.name, self.plug_in, opts))
        else:
            opts = {}
        
        return opts
    
    def _configure_nodes(self, workflow):
        """
        Sets the input parameters defined for the given workflow in
        this WorkflowBase's configuration. This method is called by
        each WorkflowBase subclass after the workflow is built and
        prior to execution.
        
        :Note: nested workflow nodes are not configured, e.g. if the
            ``registration`` workflow connects a `realign`` workflow
            node ``fnirt``, then the nested ``realign.fnirt`` node in
            ``registration`` is not configured by the parent workflow.
            The nested workflow is configured separately when the nested
            WorkflowBase subclass object is created.
        
        :param workflow: the workflow containing the nodes
        """
        # The default plug-in setting.
        if self.is_distributable and self.plug_in and self.plug_in in self.configuration:
            plugin_cfg = self.configuration[self.plug_in]
            def_plugin_args = plugin_cfg.get('plugin_args')
            if def_plugin_args and 'qsub_args' in def_plugin_args:
                # Retain this workflow's default even if a node defined
                # in this workflow is included in a parent workflow.
                def_plugin_args['overwrite'] = True
                self.logger.debug("Workflow %s default node plug-in parameters:"
                                  " %s." % (workflow.name, def_plugin_args))
        else:
            def_plugin_args = None
        
        # The nodes defined in this workflow start with the workflow name.
        prefix = workflow.name + '.'
        # Configure all node inputs.
        nodes = [workflow.get_node(name)
                 for name in workflow.list_node_names()]
        for node in nodes:
            # An input {field: value} dictionary to format a debug log message.
            input_dict = {}
            # The node configuration.
            node_cfg = self._node_configuration(workflow, node)
            # Set the node inputs or plug-in argument.
            for attr, value in node_cfg.iteritems():
                if attr == 'plugin_args':
                    # If the workflow is on a cluster and the node plug-in
                    # arguments do not overwrite the default plug-in arguments,
                    # then append the node plug-in arguments to the default
                    # and set the overwrite flag. This ensures that the node
                    # plug-in arguments take precedence over the defaults and
                    # that the arguments are retained if the node is included
                    # in a parent workflow.
                    if self.is_distributable:
                        if ('qsub_args' in value and
                                'overwrite' not in value and
                                'qsub_args' in def_plugin_args):
                            qsub_args = value['qsub_args']
                            def_qsub_args = def_plugin_args['qsub_args']
                            value['qsub_args'] = def_qsub_args + ' ' + qsub_args
                            value['overwrite'] = True
                        setattr(node, attr, value)
                        self.logger.debug("%s workflow node %s plugin"
                                          " arguments: %s" %
                                          (workflow.name, node, value))
                else:
                    # The current attribute value.
                    if hasattr(node.inputs, attr):
                        current = getattr(node.inputs, attr)
                    elif hasattr(node, attr):
                        current = getattr(node, attr)
                    else:
                        raise WorkflowError("The node %s does not have an"
                                            " attribute or input field %s" %
                                            (node, attr))
                    # If the config value differs from the default
                    # value, then capture the config entry for update.
                    if value != current:
                        input_dict[attr] = value
            
            # If:
            # 1) the configuration specifies a default,
            # 2) the node itself is not configured with plug-in arguments, and
            # 3) the node is defined in this workflow as opposed to a child
            #    workflow (i.e., the node name prefix is this workflow name),
            # then set the node plug-in arguments to the default.
            delegatable = def_plugin_args and 'plugin_args' not in node_cfg
            if delegatable and str(node).startswith(prefix):
                node.plugin_args = def_plugin_args
            
            # If a field was set to a config value, then print the config
            # setting to the log.
            if input_dict:
                self._set_node_inputs(node, **input_dict)
                self.logger.debug("The following %s workflow node %s inputs"
                                   " were set from the configuration: %s" %
                                   (workflow.name, node, input_dict))
    
    def _set_node_inputs(self, node, **opts):
        """
        Sets the given node attributes. The input attributes can be
        either a node input interface field, e.g. the
        :class:`qipipe.interfaces.copy` *dest* field, or an attribute
        of the node itself, e.g. *run_without_submitting*.
        
        :param node: the target node
        :param opts: the input {attribute: value} dictionary
        """
        # The node interface input traits.
        traits = node.inputs.traits()
        # The input interface {attribute: value} dictionary.
        input_dict = {attr: opts[attr] for attr in opts if attr in traits}
        # The input interface attributes.
        input_attrs = set(input_dict.iterkeys())
        # The {attribute: dependencies} requirement dictionary.
        req_dict = {attr: set(traits[attr].requires).intersection(input_attrs)
                    for attr in input_dict
                    if traits[attr].requires}
        # The dependency graph.
        req_grf = nx.DiGraph()
        # The dependency graph nodes.
        req_grf.add_nodes_from(input_dict)
        # The dependency graph edges.
        for attr, reqs in req_dict.iteritems():
            for req in reqs:
                req_grf.add_edge(req, attr)
        # Sort the input interface attributes by dependency.
        sorted_attrs = nx.topological_sort(req_grf)
        # Set the node input field values.
        for attr in sorted_attrs:
            setattr(node.inputs, attr, input_dict[attr])
        # Set the node attribute values.
        node_attrs = (attr for attr in opts if not attr in input_dict)
        for attr in node_attrs:
            setattr(node, attr, opts[attr])
    
    def _node_configuration(self, workflow, node):
        """
        Returns the {parameter: value} dictionary defined for the given
        node in this WorkflowBase's configuration. The configuration section
        is determined as follows:
        
        * the node class, as described in :meth:`_interface_configuration`
        
        * the node name, qualified by the node hierarchy if the node is
          defined in a child workflow
        
        :param workflow: the parent or nested workflow object
        :param node: the interface class to check
        :return: the corresponding {field: value} dictionary
        """
        return (self._interface_configuration(node.interface.__class__) or
                self._node_name_configuration(workflow, node) or EMPTY_DICT)
    
    def _node_name_configuration(self, workflow, node):
        """
        Returns the {parameter: value} dictionary defined for the given
        node name, qualified by the node hierarchy if the node is
        defined in a child workflow.
        
        :param workflow: the active workflow
        :param node: the interface class to check
        :return: the corresponding {field: value} dictionary
        """
        if node._hierarchy == workflow.name:
            return self.configuration.get(node.name)
        else:
            return self.configuration.get(node.fullname)
    
    def _interface_configuration(self, klass):
        """
        Returns the {parameter: value} dictionary defined for the given
        interface class in this WorkflowBase's configuration. The
        configuration section matches the module path of the interface class
        name. The section can elide the ``interfaces`` or ``interface`` prefix,
        e.g.:
            
            [nipype.interfaces.ants.AverageImages]
            [ants.AverageImages]
        
        both refer to the Nipype ANTS AverageImages wrapper interface.
        
        :param node: the interface class to check
        :return: the corresponding {field: value} dictionary
        """
        section = "%s.%s" % (klass.__module__, klass.__name__)
        if section in self.configuration:
            return self.configuration[section]
        elif WorkflowBase.INTERFACE_PREFIX_PAT.match(section):
            # A parent module might import the class. Therefore,
            # strip out the last module and retry.
            abbr = WorkflowBase.INTERFACE_PREFIX_PAT.sub('', section)
            while abbr:
                if abbr in self.configuration:
                    return self.configuration[abbr]
                match = WorkflowBase.MODULE_PREFIX_PAT.match(abbr)
                if match:
                    prefix, _, _, name = match.groups()
                    abbr = prefix + name
                else:
                    abbr = None
