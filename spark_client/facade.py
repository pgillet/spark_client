import logging
import os
import uuid
from io import open
from pprint import pformat

try:
    # Python 3
    from configparser import SafeConfigParser
except ImportError:
    # Python 2
    from ConfigParser import SafeConfigParser

MESOS = "mesos"
K8S_SPARK_NATIVE = "k8s_spark_native"
K8S_SPARK_OPERATOR = "k8s_spark_operator"

MANAGERS = [MESOS, K8S_SPARK_NATIVE, K8S_SPARK_OPERATOR]


class SparkJobConf(object):
    """All setter methods in this class support chaining.
    For example, you can write C{conf.env_var(...).spark_property(...).extra_property(...)}.
    """

    def __init__(self, appName=None, appResource=None, appArgs=None, mainClass=None, manager=None, configPath=None):
        """
        :param appName: The Spark app name. If not specified, a random UUID is generated.
        :param appResource: <app jar | python file>
        :param appArgs: [app arguments]
        :param mainClass: Your application's main class (for Java / Scala apps)
        :param manager: Targeted cluster manager. Accepted values are 'mesos', 'spark_native' and 'spark_operator'.
        :param configPath: Path to the configuration pipeline. If the path denotes a directory, all configuration files
        in this directory will be loaded alphabetically. Otherwise, initial and default values are first loaded from
        the 'conf/' directory before loading the regular file set by the user if any.
        """
        if configPath is None and appResource is None:
            raise Exception("Either pass a configuration file or an appResource")

        self.appName = None
        self.mainClass = None
        self.sparkProperties = {}
        self.environmentVariables = {}
        self.otherProperties = {}

        if configPath:
            self._init_config(configPath)

        if appName:
            self.appName = appName
        elif self.appName is None:
            self.appName = str(uuid.uuid4())
        if appResource:
            self.appResource = appResource
        if appArgs:
            self.appArgs = appArgs
        if mainClass:
            self.mainClass = mainClass
        if manager:
            self.manager = manager
        if self.manager is None:
            raise ValueError("Must specify a cluster manager")
        if self.manager not in MANAGERS:
            raise ValueError("Unknown cluster manager")

        self._log = logging.getLogger(__name__)

    def _init_config(self, configPath=None):
        """Process the configuration pipeline.

        :param configPath: The user configuration path
        :return:
        """
        # TODO: The SafeConfigParser class has been renamed to ConfigParser in Python 3.2.
        # This alias will be removed in future versions.
        # We still use SafeConfigParser for backwards compatibility with Python 2.
        self.config = SafeConfigParser()
        # Make option names case sensitive
        self.config.optionxform = str

        if os.path.isdir(configPath):
            # List filenames in configDir alphabetically
            _, _, configFiles = next(os.walk(configPath))
            configFiles = sorted(configFiles, key=str.lower)

            # Read configuration pipeline
            for f in configFiles:
                with open(os.path.join(configPath, f)) as configFile:
                    self.config.readfp(configFile)
                    self._store_config_pass()
        elif os.path.isfile(configPath):
            self.config.read(configPath)
            self._store_config_pass()
        else:
            raise ValueError("'{}' does not exist".format(configPath))

        appSection = 'application'
        self.appName = self._get_option_value(appSection, 'appName')
        self.appResource = self._get_option_value(appSection, 'appResource')
        self.appArgs = []
        appArgs = self._get_option_value(appSection, 'appArgs')
        if appArgs:
            self.appArgs = appArgs.split(' ')
        self.mainClass = self._get_option_value(appSection, 'mainClass')
        self.manager = self._get_option_value(appSection, 'manager')

    def _store_config_pass(self):
        """Store the configuration values from the ConfigParser into this MesosSparkJobConf's properties.
        Single-valued properties from the previous pass are overwritten.
        Multi-valued properties from the previous pass are appended.
        """
        envSection = 'environmentVariables'
        otherSection = 'other'
        propsSection = 'sparkProperties'
        for (k, v) in self.config.items(envSection):
            self.environmentVariables[k] = v
        for (k, v) in self.config.items(otherSection):
            self.otherProperties[k] = v
        for (k, v) in self.config.items(propsSection):
            self.sparkProperties[k] = v
        # Reset the section for the next pass if any
        # self.config.remove_section(propsSection)

    def _get_option_value(self, section, option):
        """Get an option value for a given section, BUT do not raise a ConfigParser.NoOptionError when the specified
        option is not found in the specified section.

        :param section:
        :param option:
        :return: None if the specified option is not found in the specified section
        """
        value = None
        if self.config.has_section(section) and self.config.has_option(section, option):
            value = self.config.get(section, option)
        return value

    def _update(self, job_conf):
        """Update this SparkJobConf with the specified one.
        Application properties, spark properties, extra properties and environment variables of the given SparkJobConf
        will override/will be inserted in the respective sections of this SparkJobConf.

        :param job_conf: a SparkJobConf
        :type job_conf: SparkJobConf
        """
        if job_conf.appName:
            self.appName = job_conf.appName
        if job_conf.appResource:
            self.appResource = job_conf.appResource
        if job_conf.appArgs is not None:
            self.appArgs = job_conf.appArgs
        if job_conf.mainClass:
            self.mainClass = job_conf.mainClass
        if job_conf.manager:
            self.manager = job_conf.manager
        self.environmentVariables.update(job_conf.environmentVariables)
        self.sparkProperties.update(job_conf.sparkProperties)
        self.otherProperties.update(job_conf.otherProperties)

    def _conf(self, a_dict, key=None, value=None, pairs=None):
        if (key is not None and pairs is not None) or (key is None and pairs is None):
            raise Exception("Either pass one key-value pair or a list of pairs")
        elif key is not None:
            a_dict[key] = value
        elif pairs is not None:
            a_dict.update(pairs)
        return self

    def env_var(self, key=None, value=None, pairs=None):
        """Set one or more environment variables to be passed to executors.

        :param key: The env variable's name
        :param value: The env variable's value
        :param pairs: Multiple key-value pairs to pass at once
        :type pairs: dict
        :return: This object itself for chaining
        """
        return self._conf(self.environmentVariables, key, value, pairs)

    def spark_property(self, key=None, value=None, pairs=None):
        """Set one or more arbitrary Spark configuration properties.

        :param key: The property's key
        :param value: The property's value
        :param pairs: Multiple key-value pairs to pass at once
        :type pairs: dict
        :return: This object itself for chaining
        """
        return self._conf(self.sparkProperties, key, value, pairs)

    def extra_property(self, key=None, value=None, pairs=None):
        """Set one or more extra properties (neither Spark properties nor environment variables).

        :param key: The property's key
        :param value: The property's value
        :param pairs: Multiple key-value pairs to pass at once
        :type pairs: dict
        :return: This object itself for chaining
        """
        return self._conf(self.otherProperties, key, value, pairs)

    def __str__(self):
        return "\n".join([
            "appName: {}".format(self.appName),
            "appResource: {}".format(self.appResource),
            "appArgs: {}".format(self.appArgs),
            "mainClass: {}".format(self.mainClass),
            "manager: {}".format(self.manager),
            "Environment variables:",
            pformat(self.environmentVariables),
            "Spark properties:",
            pformat(self.sparkProperties),
            "Extra properties:",
            pformat(self.otherProperties), ])
