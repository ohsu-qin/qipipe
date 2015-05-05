import os
import re
import tempfile
import networkx as nx
import qixnat
from qiutil.collections import EMPTY_DICT
from qiutil.ast_config import read_config
from .distributable import DISTRIBUTABLE


class WorkflowBase(object):
    """
    The WorkflowBase class is the base class for the qipipe workflow
    wrapper classes.

    If the :mod:`qipipe.pipeline.distributable' ``DISTRIBUTABLE`` flag
    is set, then the execution is distributed using the
    `AIRC Grid Engine`_.

    The workflow plug-in arguments and node inputs can be specified in a
    :class:`qiutil.ast_config.ASTConfig` file. The standard
    configuration file name is the lower-case name of the ``WorkflowBase``
    subclass with ``.cfg`` extension, e.g. ``registration.cfg``. The
    configuration file paths to load in low-to-high precedence order
    consist of the following:

    1. the default config file ``default.cfg`` in the qipipe project
       ``conf`` directory

    2. the standard config file name in the qipipe project ``conf``
       directory

    3. the standard config file name in the current working directory

    4. the standard config file name in the ``QIN_CONF`` environment
       variable directory

    5. the *config* initialization parameter

    .. _AIRC Grid Engine: https://everett.ohsu.edu/wiki/GridEngine
    """

    CLASS_NAME_PAT = re.compile("^(\w+)Workflow$")
    """The workflow wrapper class name matcher."""

    DEF_CONF_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'conf')
    """The default configuration directory."""

    CFG_ENV_VAR='QIN_CONF'
    """The configuration directory environment variable."""

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

    def __init__(self, project, logger, **opts):
        """
        Initializes this workflow wrapper object.
        If the optional configuration file is specified, then the workflow
        settings in that file override the default settings.

        :param project: the XNAT project name
        :param logger: the logger to use
        :parameter opts: the following keword options:
        :keyword config: the optional workflow inputs configuration
            dictionary or file
        :keyword dry_run: flag indicating whether to prepare but not
            run the pipeline
        """
        self.project = project
        
        self._logger = logger

        cfg_opt = opts.get('config', None)
        if cfg_opt == None or isinstance(cfg_opt, str):
            config = self._load_configuration(cfg_opt)
        else:
            config = cfg_opt
        self.configuration = config
        """The workflow configuration."""

        self.dry_run = opts.get('dry_run', False)
        """Flag indicating whether to skip workflow job submission."""

    def depict_workflow(self, workflow):
        """
        Diagrams the given workflow graph. The diagram is written to the
        *name*\ ``.dot.png`` in the workflow base directory.

        :param workflow the workflow to diagram
        """
        fname = workflow.name + '.dot'
        if workflow.base_dir:
            grf = os.path.join(workflow.base_dir, fname)
        else:
            grf = fname
        workflow.write_graph(dotfilename=grf)
        self._logger.debug("The %s workflow graph is depicted at %s.png." %
                         (workflow.name, grf))

    def _load_configuration(self, cfg_file=None):
        """
        Loads the workflow configuration, as defined in
        :class:`qipipe.pipeline.workflow_base.WorkflowBase`.

        :param cfg_file: the optional configuration file path
        :return: the configuration dictionary
        """
        # The configuration files to load.
        cfg_files = []

        # The default configuration file.
        def_cfg_file = os.path.join(WorkflowBase.DEF_CONF_DIR, 'default.cfg')
        if os.path.exists(def_cfg_file):
            cfg_files.append(os.path.abspath(def_cfg_file))

        # Validate that the workflow name ends in Workflow.
        match = WorkflowBase.CLASS_NAME_PAT.match(self.__class__.__name__)
        if not match:
            raise NameError("The workflow wrapper class does not match the"
                            " standard workflow class name pattern: %s" %
                            self.__class__.__name__)
        name = match.group(1)
        fname = "%s.cfg" % name.lower()
        
        # The workflow-specific configuration file is in the conf directory.
        base_cfg_file = os.path.join(WorkflowBase.DEF_CONF_DIR, fname)
        if os.path.exists(base_cfg_file):
            cfg_files.append(os.path.abspath(base_cfg_file))

        # The working directory config file.
        cwd_cfg_file = os.path.abspath(fname)
        if os.path.exists(cwd_cfg_file) and cwd_cfg_file not in cfg_files:
            cfg_files.append(cwd_cfg_file)

        # The config file specified by the directory environment variable.
        env_cfg_dir = os.getenv(WorkflowBase.CFG_ENV_VAR, None)
        if env_cfg_dir:
            env_cfg_file = os.path.abspath(os.path.join(env_cfg_dir, fname))
            if os.path.exists(env_cfg_file) and env_cfg_file not in cfg_files:
                cfg_files.append(env_cfg_file)

        # The argument config file.
        if cfg_file:
            cfg_file = os.path.abspath(cfg_file)
            if os.path.exists(cfg_file) and cfg_file not in cfg_files:
                cfg_files.append(cfg_file)

        # Load the configuration.
        if cfg_files:
            self._logger.debug("Loading the %s configuration files %s..." %
                             (name, cfg_files))
            cfg = read_config(*cfg_files)
            return dict(cfg)
        else:
            return {}

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
        if DISTRIBUTABLE:
            opts = self._configure_plugin(workflow)
        else:
            opts = {}

        # Set the base directory to an absolute path.
        if workflow.base_dir:
            workflow.base_dir = os.path.abspath(workflow.base_dir)
        else:
            workflow.base_dir = tempfile.mkdtemp()

        # Run the workflow.
        self._logger.debug("Executing the %s workflow in %s..." %
                           (workflow.name, workflow.base_dir))
        if self.dry_run:
            self._logger.debug("Skipped workflow %s job submission,"
                               " since the dry run flag is set." %
                               workflow.name)
        else:
            with qixnat.connect():
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
        Sets the *execution* and *SGE* parameters for the given workflow.
        See the ``conf`` directory files for examples.

        :param workflow: the workflow to run
        :return: the workflow execution arguments
        """
        # The workflow submission arguments:

        # The execution setting.
        if 'Execution' in self.configuration:
            workflow.config['execution'] = self.configuration['Execution']
            self._logger.debug("Workflow %s execution parameter: %s." %
                             (workflow.name, workflow.config['execution']))

        # The Grid Engine setting.
        if 'SGE' in self.configuration:
            opts = dict(plugin='SGE', **self.configuration['SGE'])
            self._logger.debug("Workflow %s plug-in parameters: %s." %
                             (workflow.name, opts))
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
        ``registration`` is not configured.

        :param workflow: the workflow containing the nodes
        """
        # The default Grid Engine setting.
        if DISTRIBUTABLE and 'SGE' in self.configuration:
            def_plugin_args = self.configuration['SGE'].get('plugin_args')
            if def_plugin_args and 'qsub_args' in def_plugin_args:
                # Retain this workflow's default even if a node defined
                # in this workflow is included in a parent workflow.
                def_plugin_args['overwrite'] = True
                self._logger.debug("Workflow %s default node plug-in parameters:"
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
            cfg = self._node_configuration(workflow, node)

            # Set the node inputs or plug-in argument.
            for attr, value in cfg.iteritems():
                if attr == 'plugin_args':
                    # If the workflow is on a cluster and the node plug-in
                    # arguments do not overwrite the default plug-in arguments,
                    # then append the node plug-in arguments to the default
                    # and set the overwrite flag. This ensures that the node
                    # plug-in arguments take precedence over the defaults and
                    # that the arguments are retained if the node is included
                    # in a parent workflow.
                    if DISTRIBUTABLE:
                        if ('qsub_args' in value and
                                'overwrite' not in value and
                                'qsub_args' in def_plugin_args):
                            qsub_args = value['qsub_args']
                            def_qsub_args = def_plugin_args['qsub_args']
                            value['qsub_args'] = def_qsub_args + ' ' + qsub_args
                            value['overwrite'] = True
                        setattr(node, attr, value)
                        self._logger.debug("%s workflow node %s plugin"
                                          " arguments: %s" %
                                          (workflow.name, node, value))
                elif value != getattr(node.inputs, attr):
                    # The config value differs from the default value.
                    # Capture the config entry for update.
                    input_dict[attr] = value
            
            # If:
            # 1) the configuration specifies a default,
            # 2) the node itself is not configured with plug-in arguments, and
            # 3) the node is defined in this workflow as opposed to a child
            #    workflow (i.e., the node name prefix is this workflow name),
            # then set the node plug-in arguments to the default.
            if (def_plugin_args and 'plugin_args' not in cfg and
                    str(node).startswith(prefix)):
                node.plugin_args = def_plugin_args

            # If a field was set to a config value, then print the config
            # setting to the log.
            if input_dict:
                self._set_inputs(node, input_dict)
                self._logger.debug("The following %s workflow node %s inputs"
                                   " were set from the configuration: %s" %
                                   (workflow.name, node, input_dict))

    def _set_inputs(self, node, input_dict):
        # Sort the input config fields by the requires dependency.
        traits = node.inputs.traits()
        attrs = set(input_dict)
        req_dict = {attr: set(traits[attr].requires).intersection(attrs)
                    for attr in input_dict
                    if traits[attr].requires}
        req_grf = nx.DiGraph()
        req_grf.add_nodes_from(input_dict)
        for attr, reqs in req_dict.iteritems():
            for req in reqs:
                req_grf.add_edge(req, attr)
        sorted_attrs = nx.topological_sort(req_grf)
        # Set the input config fields.
        for attr in sorted_attrs:
            setattr(node.inputs, attr, input_dict[attr])

    def _node_configuration(self, workflow, node):
        """
        Returns the {parameter: value} dictionary defined for the given
        node in this WorkflowBase's configuration. The configuration topic
        is determined as follows:

        * the node class, as described in :meth:`_interface_configuration`

        * the node name, qualified by the node hierarchy if the node is
          defined in a child workflow

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
        configuration topic matches the module path of the interface class
        name. The topic can elide the ``interfaces`` or ``interface`` prefix,
        e.g.:

            [nipype.interfaces.ants.AverageImages]
            [ants.AverageImages]

        both refer to the ANTS AverageImages interface.

        :param node: the interface class to check
        :return: the corresponding {field: value} dictionary
        """
        topic = "%s.%s" % (klass.__module__,
                           klass.__name__)
        if topic in self.configuration:
            return self.configuration[topic]
        elif WorkflowBase.INTERFACE_PREFIX_PAT.match(topic):
            # A parent module might import the class. Therefore,
            # strip out the last module and retry.
            abbr = WorkflowBase.INTERFACE_PREFIX_PAT.sub('', topic)
            while abbr:
                if abbr in self.configuration:
                    return self.configuration[abbr]
                match = WorkflowBase.MODULE_PREFIX_PAT.match(abbr)
                if match:
                    prefix, _, _, name = match.groups()
                    abbr = prefix + name
                else:
                    abbr = None
