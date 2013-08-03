import os, re
import logging
import nipype.pipeline.engine as pe
from ..helpers.project import project
from ..helpers import xnat_helper
from ..helpers.ast_config import read_config
from .distributable import DISTRIBUTABLE

class WorkflowBase(object):
    """
    The WorkflowBase class is the base class for the QIN workflow wrapper classes.
    """
    
    CLASS_NAME_PAT = re.compile("^(\w+)Workflow$")
    """The workflow wrapper class name matcher."""
    
    DEF_CONF_DIR = os.path.join(os.path.dirname(__file__),'..', '..', 'conf')
    """The default configuration directory."""
    
    def __init__(self, logger, cfg_file=None):
        """
        Initializes this workflow wrapper object.
        If the optional configuration file is specified, then the workflow settings
        in that file override the default settings.
        
        :param logger: the logger to use
        :param cfg_file: the optional workflow inputs configuration file
        """
        self.logger = logger
        self.configuration = self._load_configuration(cfg_file)
        """The workflow configuration."""
    
    def _load_configuration(self, cfg_file=None):
        """
        Loads the workflow configuration. The default configuration resides in
        the project ``conf`` directory. If an configuration file is specified,
        then the settings in that file override the default settings.
        
        :param cfg_file: the optional configuration file path
        :return: the configuration dictionary
        """
        # The default configuration file is in the conf directory.
        match = WorkflowBase.CLASS_NAME_PAT.match(self.__class__.__name__)
        if not match:
            raise NameError("The workflow wrapper class does not match the standard"
                " workflow class name pattern: %s" % self.__class__.__name__)
        name = match.group(1)
        fname = "%s.cfg" % name.lower()
        def_cfg_file = os.path.join(WorkflowBase.DEF_CONF_DIR, fname)
        
        # The configuration files to load.
        cfg_files = []
        if os.path.exists(def_cfg_file):
            cfg_files.append(def_cfg_file)
        if cfg_file:
            cfg_files.append(cfg_file)
        
        # Load the configuration.
        if cfg_files:
            self.logger.debug("Loading the %s configuration files %s..." %
                (name, cfg_files))
            cfg = read_config(*cfg_files)
            return dict(cfg)
        else:
            return {}
        
    def _download_scans(self, xnat, subject, session, dest):
        """
        Download the NIFTI scan files for the given session.
        
        :param xnat: the :class:`qipipe.helpers.xnat_helper.XNAT` connection
        :param subject: the XNAT subject label
        :param session: the XNAT session label
        :param dest: the destination directory path
        :return: the download file paths
        """
        return xnat.download(project(), subject, session, dest=dest,
            container_type='scan', format='NIFTI')
    
    def _depict_workflow(self, workflow):
        """Diagrams the given workflow graph."""
        if workflow.base_dir:
            grf = os.path.join(workflow.base_dir, 'staging.dot')
        else:
            grf = 'staging.dot'
        workflow.write_graph(dotfilename=grf)
        self.logger.debug("The %s workflow graph is depicted at %s.png." %
            (workflow.name, grf))
        
    def _run_workflow(self, workflow):
        """
        Executes the given workflow.
        
        :param workflow: the workflow to run
        """
        # The workflow submission arguments.
        args = {}
        # Check whether the workflow can be distributed.
        if DISTRIBUTABLE:
            # Distribution parameters collected for a debug message.
            log_msg_params = {}
            # The execution setting.
            if 'execution' in self.configuration:
                workflow.config['execution'] = self.configuration['execution']
                log_msg_params.update(self.configuration['execution'])
            # The Grid Engine setting.
            if 'SGE' in self.configuration:
                args = dict(plugin='SGE', plugin_args=self.configuration['SGE'])
                log_msg_params.update(self.configuration['SGE'])
            # Print a debug message.
            if log_msg_params:
                self.logger.debug("Submitting the %s workflow to the Grid Engine with parameters %s..." %
                    (workflow.name, log_msg_params))
        
        # Run the workflow.
        with xnat_helper.connection():
            workflow.run(**args)
