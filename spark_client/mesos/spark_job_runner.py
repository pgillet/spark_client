from __future__ import absolute_import

import logging
import os
import time
import uuid
import warnings
from io import open

from pymesos import MesosOperatorMasterDriver, MesosOperatorAgentDriver
from spark_client.facade import SparkJobConf, MESOS
from spark_client.interface import SparkJobRunner
from spark_client.mesos.spark_client import DriverState
from spark_client.mesos.spark_client import JobSubmitRequest
from spark_client.mesos.spark_client import SparkClient
from spark_client.mesos.spark_client import SparkProperties

try:
    # Python 3
    from configparser import SafeConfigParser
    from urllib.parse import urlencode
    from urllib.request import urlopen, Request
except ImportError:
    # Python 2
    from ConfigParser import SafeConfigParser
    from urllib import urlencode
    from urllib2 import urlopen, Request

# Separators for multi-valued Spark properties
COMMA = ','
SEMICOLON = ';'
SPACE = ' '


class MesosSparkJobConf(object):
    """
    Deprecated: please use spark_client.facade.SparkJobConf which only keeps basic methods to set Spark properties and
    environment variables, regardless of the targeted cluster manager.
    
    All setter methods in this class support chaining. For example, you can write C{conf.docker(...).driver(...).executor(...)}.
    """

    # A dict that maps multi-valued Spark properties to the separator used to separate elements
    _multi_valued_spark_properties = {SparkProperties.SPARK_SUBMIT_PY_FILES: COMMA,
                                      SparkProperties.SPARK_JARS: COMMA,
                                      SparkProperties.SPARK_DRIVER_EXTRACLASSPATH: COMMA,
                                      SparkProperties.SPARK_EXECUTOR_EXTRACLASSPATH: COMMA,
                                      SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_VOLUMES: COMMA,
                                      SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_PARAMETERS: COMMA,
                                      SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_PORTMAPS: COMMA,
                                      SparkProperties.SPARK_DRIVER_EXTRAJAVAOPTIONS: SPACE,
                                      SparkProperties.SPARK_EXECUTOR_EXTRAJAVAOPTIONS: SPACE, }

    # SparkProperties.SPARK_MESOS_CONSTRAINTS: SEMICOLON,
    # SparkProperties.SPARK_MESOS_DRIVER_CONSTRAINTS: SEMICOLON,}

    def __init__(self, appName=None, appResource=None, appArgs=None, mainClass='DUMMY', configPath=None,
                 mesosLoggingEnabled=True):
        """
        :param appName: The Spark app name. If not specified, a random UUID is generated.
        :param appResource: <app jar | python file>
        :param appArgs: [app arguments]
        :param mainClass: Your application's main class (for Java / Scala apps)
        :param configPath: Path to the configuration pipeline. If the path denotes a directory, all configuration files
        in this directory will be loaded alphabetically. Otherwise, initial and default values are first loaded from
        the 'conf/' directory before loading the regular file set by the user if any.
        :param mesosLoggingEnabled: Tells whether the Spark logs should be formatted so as to include the workflow
        ID (appName), allowing to monitor from end to end a particular job.
        """
        if configPath is None and appResource is None:
            raise Exception("Either pass a configuration file or an appResource")

        self.jars = None
        self.pyFiles = None
        self.sparkProperties = {}
        self.environmentVariables = {}
        self.otherProperties = {}

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

        # if mesosLoggingEnabled:
        #     self._configure_logging()

        self._log = logging.getLogger(__name__)

    def _configure_logging(self):
        """
        Set Java system properties that will be substituted in the Spark's Log4j configuration file.
        Set also environment variables for configuring PySpark logging format.
        """
        workflowProperty = '-Dapp.workflow={}'.format(self.appName)

        # self.sparkProperties[SparkProperties.SPARK_DRIVER_EXTRAJAVAOPTIONS] = workflowProperty
        # self.sparkProperties[SparkProperties.SPARK_EXECUTOR_EXTRAJAVAOPTIONS] = workflowProperty

        self._append_or_create_property(SparkProperties.SPARK_DRIVER_EXTRAJAVAOPTIONS, workflowProperty, ' ')
        self._append_or_create_property(SparkProperties.SPARK_EXECUTOR_EXTRAJAVAOPTIONS, workflowProperty, ' ')
        # Add environment variables to the executors
        self.extra_property('spark.executorEnv.APP_WORKFLOW', self.appName)
        # Add environment variables to the driver process. TODO: with executor_env ?
        self.executor_env('APP_WORKFLOW', self.appName)

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

        if configPath and os.path.isdir(configPath):
            configDir = configPath
        else:
            configDir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'conf')

        # List filenames in configDir alphabetically
        _, _, configFiles = next(os.walk(configDir))
        configFiles = sorted(configFiles, key=str.lower)

        # Read configuration pipeline
        for f in configFiles:
            with open(os.path.join(configDir, f)) as configFile:
                self.config.readfp(configFile)
                self._store_config_pass()

        if configPath and os.path.isfile(configPath):
            self.config.read(configPath)
            self._store_config_pass()

        appSection = 'application'
        self.appName = self._get_option_value(appSection, 'appName')
        self.appResource = self._get_option_value(appSection, 'appResource')
        self.appArgs = []
        appArgs = self._get_option_value(appSection, 'appArgs')
        if appArgs:
            self.appArgs = appArgs.split(' ')
        self.mainClass = self._get_option_value(appSection, 'mainClass')

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
            # Special cases for some multi-valued properties
            if k in MesosSparkJobConf._multi_valued_spark_properties:
                sep = MesosSparkJobConf._multi_valued_spark_properties[k]
                self._append_or_create_property(k, v, sep)
            else:
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
            value = self.appName = self.config.get(section, option)
        return value

    def docker(self, image, forcePullImage=None, volumes=None, portMaps=None):
        """Use a Docker image to run this Spark job.

        :param image: The name of the Docker image that the Spark executors will run in. The selected
        :param forcePullImage: True to force Mesos agents to pull the specified image. False by default.
        :param volumes: the list of volumes which will be mounted in the Docker image. Comma-separated list of mappings
        following the form [host_path:]container_path[:ro|rw]
        :param portMaps: the list of upcoming ports exposed by the specified Docker image. Comma-separated list of
        mappings following the form host_port:container_port[:tcp|:udp]
        :return: This object itself for chaining
        """
        self.sparkProperties[SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_IMAGE] = image
        if forcePullImage:
            self.sparkProperties[
                SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_FORCEPULLIMAGE] = forcePullImage
        if volumes:
            self.sparkProperties[
                SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_VOLUMES] = volumes
        if portMaps:
            self.sparkProperties[
                SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_PORTMAPS] = portMaps
        if parameters:
            self.sparkProperties[
                SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_PARAMETERS] = parameters
        return self

    def _volume(self, volume):
        """Bind mount a Docker volume.
        Calling this method makes sense only if the docker method is called first.
        You can call this method multiple times to mount multiple data volumes.

        :param volume: A volume to be added to the list of volumes which will be mounted in the Docker image specified
        with the docker method. The volume must have the form [host_path:]container_path[:ro|rw]
        :return: This object itself for chaining
        """
        self._append_or_create_property(SparkProperties.SPARK_MESOS_EXECUTOR_DOCKER_VOLUMES, volume)
        return self

    def volume(self, hostPath, containerPath, readOnly=False):
        """
        Bind mount a Docker volume.
        Calling this method makes sense only if the docker method is called first.
        You can call this method multiple times to mount multiple data volumes.

        :param hostPath: Host path
        :type hostPath: str
        :param containerPath: Container path
        :type containerPath: str
        :param readOnly: True to specify that the mount should be read-only, False for read-write
        :type readOnly: bool
        :return:
        """
        volumeSpec = hostPath + ':' + containerPath
        if readOnly:
            volumeSpec += ':ro'

        return self._volume(volumeSpec)

    def executor(self, extraClassPath=None):
        """Configuration for executors.
        TODO: This method will be surely extended with other Spark parameters

        :param extraClassPath: Extra classpath entries to prepend to the classpath of executors
        :return: This object itself for chaining
        """
        if extraClassPath:
            self.sparkProperties[SparkProperties.SPARK_EXECUTOR_EXTRACLASSPATH] = extraClassPath
        return self

    def coarse_grained(self, memory=None, cores=None, maxCores=None):
        """Configuration of executors in coarse-grained mode.

        :param cores: Number of cores to allocate per executor. Setting this parameter allows an application to run
        multiple executors on the same worker, provided that there are enough cores on that worker. Otherwise, only one
        executor per application will run on each worker.
        :param memory: Amount of memory to use per executor (e.g. 2048M, 1g)
        :param maxCores: The maximum amount of CPU cores to request for the application from across the cluster (not
        from each machine). If not set, the default is all available cores on Mesos.
        :return: This object itself for chaining
        """
        if memory:
            self.sparkProperties[SparkProperties.SPARK_EXECUTOR_MEMORY] = memory
        if cores:
            self.sparkProperties[SparkProperties.SPARK_EXECUTOR_CORES] = cores
        if maxCores:
            self.sparkProperties[SparkProperties.SPARK_CORES_MAX] = maxCores
        return self

    def dynamic_alloc(self, minExecutors=None, maxExecutors=None, initialExecutors=None, fairScheduling=False,
                      maxCores=None):
        """Enable dynamic resource allocation, which scales the number of executors registered with this application up
        and down based on the workload.

        :param minExecutors: Lower bound for the number of executors
        :param maxExecutors: Upper bound for the number of executors
        :param initialExecutors: Initial number of executors to run
        :param fairScheduling: The scheduling mode between jobs submitted to the same SparkContext. True for FAIR
        sharing, False for queuing jobs one after another.
        :type fairScheduling: bool
        :param maxCores: The maximum amount of CPU cores to request for the application from across the cluster (not
        from each machine). If not set, the default is all available cores on Mesos.
        TODO: Is this parameter relevant with dynamic allocation?

        :return: This object itself for chaining
        """
        self.sparkProperties[SparkProperties.SPARK_DYNAMICALLOCATION_ENABLED] = 'true'
        self.sparkProperties[SparkProperties.SPARK_SHUFFLE_SERVICE_ENABLED] = 'true'
        if maxCores:
            self.sparkProperties[SparkProperties.SPARK_CORES_MAX] = maxCores
        if initialExecutors:
            self.sparkProperties[SparkProperties.SPARK_DYNAMICALLOCATION_INITIALEXECUTORS] = initialExecutors
        if minExecutors:
            self.sparkProperties[SparkProperties.SPARK_DYNAMICALLOCATION_MINEXECUTORS] = minExecutors
        if maxExecutors:
            self.sparkProperties[SparkProperties.SPARK_DYNAMICALLOCATION_MAXEXECUTORS] = maxExecutors
        if fairScheduling:
            self.sparkProperties[SparkProperties.SPARK_SCHEDULER_MODE] = "FAIR"
        return self

    def driver(self, cores=1, memory=None, extraClassPath=None):
        """Configuration for the Spark driver

        :param cores: Number of cores to use for the driver process.
        :param memory: Amount of memory to use for the driver process
        :param extraClassPath: Extra classpath entries to prepend to the classpath of the driver
        :return: This object itself for chaining
        """
        if cores:
            self.sparkProperties[SparkProperties.SPARK_DRIVER_CORES] = cores
        if memory:
            self.sparkProperties[SparkProperties.SPARK_DRIVER_MEMORY] = memory
        if extraClassPath:
            self.sparkProperties[SparkProperties.SPARK_DRIVER_EXTRACLASSPATH] = extraClassPath
        return self

    def java_path(self, jars):
        """Set your application's dependencies. For Java apps only.

        :param jars: Comma-separated list of local jars to include on the driver an executor classpaths.
        :return: This object itself for chaining
        """
        self.jars = jars
        return self

    def python_path(self, pyFiles):
        """Set your application's dependencies. For Python apps only.


        :param pyFiles: Comma-separated list of .zip, .egg or .py files to place on the PYTHONPATH for Python apps.
        :return: This object itself for chaining
        """
        self.pyFiles = pyFiles
        return self

    def executor_env(self, key=None, value=None, pairs=None):
        """Set one or more environment variables to be passed to executors.
        """
        if (key is not None and pairs is not None) or (key is None and pairs is None):
            raise Exception("Either pass one key-value pair or a list of pairs")
        elif key is not None:
            self.environmentVariables[key] = value
        elif pairs is not None:
            for (k, v) in pairs:
                self.environmentVariables[k] = v
        return self

    def history(self, logDir=None, historyServerUrl=None, dispatcherWebuiUrl=None):
        """History configuration, integrated by default with OpenStack Swift.

        :param logDir: Base directory in which Spark events are logged
        :param historyServerUrl: The history server URL. The dispatcher will then link each driver to its entry in the
        history server
        :param dispatcherWebuiUrl: The Spark Mesos dispatcher Web UI URL
        :return: This object itself for chaining
        """
        self._log.warn(
            'In case of integration with OpenStack Swift for storing Spark logs, the Docker image should either embed \
            the hadoop-openstack jar dependency, or simply mount the jar file when running the image.')
        self._log.warn(
            'Mount also a core-site.xml file in the Docker image and place it inside Spark\'s conf directory to \
            configure the connection to the Swift file system')
        self.sparkProperties[SparkProperties.SPARK_EVENTLOG_ENABLED] = 'true'
        if logDir:
            self.sparkProperties[SparkProperties.SPARK_EVENTLOG_DIR] = logDir
        if historyServerUrl:
            self.sparkProperties[SparkProperties.SPARK_MESOS_DISPATCHER_HISTORYSERVER_URL] = historyServerUrl
        if dispatcherWebuiUrl:
            self.sparkProperties[SparkProperties.SPARK_MESOS_DISPATCHER_WEBUI_URL] = dispatcherWebuiUrl
        return self

    def spark_home(self, sparkHome):
        """Set the directory in which Spark is installed on the executors in Mesos.

        :param sparkHome:
        :return: This object itself for chaining
        """
        self.sparkProperties[SparkProperties.SPARK_MESOS_EXECUTOR_HOME] = sparkHome
        return self

    def spark_user(self, sparkUser):
        """The Spark job submitted to Mesos will be run with the specified user.

        :param sparkUser:
        :return: This object itself for chaining
        """
        self.executor_env(key='SPARK_USER', value=sparkUser)
        return self

    def priority(self, clazz, weight):
        """
        Deprecated: Use role method instead.
        Set the role of this Spark framework for Mesos, for reservations and resource weight sharing.

        :param clazz: e.g. 'ROUTINE', 'URGENT', 'EXCEPTIONAL'
        :param weight: an int greater or equal to 1
        :return: This object itself for chaining
        """
        warnings.warn('The \'priority\' method is deprecated, use \'role\' instead', DeprecationWarning)
        # priorityHandler = PriorityHandler()
        # mesosRole = priorityHandler.encode(clazz, weight)
        # self.sparkProperties[SparkProperties.SPARK_MESOS_ROLE] = mesosRole
        self.role(clazz)
        return self

    def role(self, roleSpec):
        """Set the role of this Spark framework for Mesos without any check of the given literal, for reservations and
        resource weight sharing.
=
        :param roleSpec:
        :return: This object itself for chaining
        """
        self.sparkProperties[SparkProperties.SPARK_MESOS_ROLE] = roleSpec
        return self

    def extra_property(self, key, value, append=False):
        """Set an arbitrary configuration property that cannot be set with other methods.

        :param key: The property's key
        :param value: The property's value
        :param append: True to append the given value to the property and if the property is actually multi-valued,
        False to create or override the property
        :return: This object itself for chaining
        """
        if append and key in MesosSparkJobConf._multi_valued_spark_properties:
            sep = MesosSparkJobConf._multi_valued_spark_properties[key]
            self._append_or_create_property(key, value, sep)
        else:
            self.sparkProperties[key] = value
        return self

    def extra_properties(self, extraProps, append=False):
        """Set arbitrary configuration properties that cannot be set with other methods.

        :param extraProps:
        :type extraProps: dict
        :param append: See extra_property
        :return: This object itself for chaining
        """
        if append:
            for k, v in extraProps.items():
                self.extra_property(k, v, append)
        else:
            self.sparkProperties.update(extraProps)
        return self

    def _append_or_create_property(self, key, value, separator=','):
        """
        Utility method that appends the specified value in the sparkProperties dictionary at the given key if the key is
        already present, creates the entry otherwise.
        """
        if key in self.sparkProperties:
            # Append value to the existing entry
            self.sparkProperties[key] += separator + value
        else:
            # Create a new entry
            self.sparkProperties[key] = value


class MesosSparkJobRunner(SparkJobRunner):
    DEFAULT_MASTER = 'mesos://spark-dispatcher.spark.marathon.mesos:7077'
    DEFAULT_MESOS_MASTER = 'leader.mesos:5050'
    SPARK_VERSION = '3.0.0'

    def __init__(self, mesos_master=DEFAULT_MESOS_MASTER, spark_master=DEFAULT_MASTER,
                 client_spark_version=SPARK_VERSION):
        """
        :param mesos_master: The Mesos master. 'leader.mesos:5050' by default.
        :param spark_master: The cluster manager to connect to, in the following format: mesos://HOST:PORT.
        In cluster deploy mode, the HOST:PORT should be configured to connect to the MesosClusterDispatcher.
        :param client_spark_version: The version of Spark used.
        """
        super(MesosSparkJobRunner, self).__init__()
        self._log = logging.getLogger(__name__)
        self.mesos_master = mesos_master
        self.driver = MesosOperatorMasterDriver(self.mesos_master)

        # Master URL can also be specified in conf in mesos://HOST:PORT format
        prefix = 'mesos://'
        spark_master = spark_master[len(prefix):]
        host_port = spark_master.split(':')
        self._client = SparkClient(host_port[0], host_port[1], client_spark_version)

    @SparkJobRunner.job_conf.setter
    def job_conf(self, value):
        # Apply given conf over root configuration
        self._job_conf = SparkJobConf(manager=MESOS,
                                      configPath=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'conf'))
        self._job_conf._update(value)

    def run(self):
        """
            See SparkClient#submit method
            :return:
        """
        sparkProperties = SparkProperties(self.job_conf.appName, otherSparkProperties=self.job_conf.sparkProperties)
        jobSubmitRequest = JobSubmitRequest(appResource=self.job_conf.appResource,
                                            appArgs=self.job_conf.appArgs,
                                            sparkProperties=sparkProperties,
                                            environmentVariables=self.job_conf.environmentVariables)
        res = self._client.submit(jobSubmitRequest)
        return res

    def kill(self, submissionId):
        """
            See SparkClient#kill method
        """
        return self._client.kill(submissionId)

    def status(self, submissionId):
        """
            See SparkClient#status method
        """
        return self._client.status(submissionId)

    def mesos_status(self, submissionId):
        """
            Returns the state of the Mesos task with the given ID.
            :param submissionId: The Spark job ID
            :return:
        """
        get_tasks = self.driver.getTasks()['get_tasks']
        task_state = None

        tasks = get_tasks['tasks'] + get_tasks.get('completed_tasks')
        tasks_list = list(filter(lambda x: x['task_id']['value'] == submissionId, tasks))
        if len(tasks_list) > 0:
            task = tasks_list[0]
            task_state = task['state']
            self._log.debug("Task state = " + task_state)
        else:
            self._log.debug("Task not found")

        return task_state

    def is_finished(self, submissionId):
        return self.status(submissionId) == DriverState.FINISHED

    def is_killed(self, submissionId):
        return self.status(submissionId) == DriverState.KILLED

    def is_running(self, submissionId):
        return self.status(submissionId) == DriverState.RUNNING

    def is_queued(self, submissionId):
        return self.status(submissionId) == DriverState.QUEUED

    def _has_state(self, submissionId, states):
        """
        Returns True if the specified job is in one of the given states, False otherwise.
        """
        status = self.status(submissionId)
        self._log.debug('Current state of job {}: {}'.format(submissionId, status))
        return status in states

    def is_completed(self, submissionId):
        """
        Returns True if the specified job is in one of the final states (UNKNOWN, FINISHED, KILLED, ERROR or FAILED),
        False otherwise.
        """
        return self._has_state(submissionId,
                               [DriverState.UNKNOWN, DriverState.FINISHED, DriverState.KILLED, DriverState.ERROR,
                                DriverState.FAILED, DriverState.NOT_FOUND])

    def is_not_queued(self, submissionId):
        status = self.status(submissionId)
        return (status != DriverState.QUEUED) and (status != DriverState.NOT_FOUND)

    def wait_until_completed(self, submissionId, timeout=0, period=5):
        print (self.status(submissionId))
        start = time.time()
        end = start + timeout
        while timeout <= 0 or time.time() < end:
            if self.is_completed(submissionId):
                return True
            time.sleep(period)
        return False

    def wait_until_start(self, submissionId, timeout=0, period=5):
        start = time.time()
        end = start + timeout
        while timeout <= 0 or time.time() < end:
            if self.is_not_queued(submissionId):
                return True
            time.sleep(period)
        return False

    def _mesos_task_info(self, submissionId):
        """
            Returns the Mesos Agent ID, Agent hostname and port, Framework ID and Container ID based on an Spark job ID

            :param submissionId: The Spark job ID
            :return: The Mesos Agent ID, Agent hostname and port, Framework ID and Container ID. None if not yet
            available or if the submissionID does not refer to any existing application
        """
        agent_id = agent_hostname = agent_port = framework_id = container_id = None
        get_state = self.driver.getState()['get_state']
        get_tasks = get_state['get_tasks']

        tasks = get_tasks['tasks'] + get_tasks.get('completed_tasks', [])
        tasks_list = list(filter(lambda x: x['task_id']['value'] == submissionId, tasks))
        if len(tasks_list) > 0:
            task = tasks_list[0]
            agent_id = task['agent_id']['value']
            framework_id = task['framework_id']['value']

        if agent_id is not None:
            get_agents = get_state['get_agents']
            agents = get_agents['agents']
            agents_list = list(filter(lambda x: x['agent_info']['id']['value'] == agent_id, agents))
            if len(agents_list) > 0:
                agent = agents_list[0]
                agent_hostname = agent['agent_info']['hostname']
                agent_port = agent['agent_info']['port']
                agent_driver = MesosOperatorAgentDriver('{}:{}'.format(agent_hostname, agent_port))
                containers = agent_driver.getContainers()['get_containers']['containers']
                containers_list = list(filter(lambda x: x['executor_id']['value'] == submissionId, containers))
                if len(containers_list) > 0:
                    container = containers_list[0]
                    container_id = container['container_id']['value']

        return agent_id, agent_hostname, str(agent_port), framework_id, container_id

    def urls(self, submissionId, watch=False, timeout=0, period=5):
        dispatcher_url = self._client._urljoin(
            self.job_conf.sparkProperties[SparkProperties.SPARK_MESOS_DISPATCHER_WEBUI_URL],
            'driver', '?id=' + submissionId)

        agent_id, agent_hostname, agent_port, framework_id, container_id = self._mesos_task_info(submissionId)
        if watch:
            start = time.time()
            end = start + timeout
            while framework_id is None and (timeout <= 0 or time.time() < end):
                agent_id, agent_hostname, agent_port, framework_id, container_id = self._mesos_task_info(submissionId)
                time.sleep(period)

        history_server_url = mesos_sandbox_url = stdout = stderr = None
        if framework_id is not None:
            history_server_url = self._client._urljoin(
                self.job_conf.sparkProperties[SparkProperties.SPARK_MESOS_DISPATCHER_HISTORYSERVER_URL], 'history',
                framework_id + '-' + submissionId)

            # Mesos sandbox URL
            # root ('--work_dir') / slaves / <agent ID> / frameworks / <framework ID> / executors / <executor ID> / runs / <container ID>
            work_dir = '/var/lib/mesos'  # root ('--work_dir')
            sandbox_path = os.path.join(work_dir, 'slaves', agent_id, 'frameworks', framework_id, 'executors',
                                        submissionId, 'runs', container_id)
            params = {'path': sandbox_path}
            query_string = urlencode(params)
            mesos_sandbox_url = self._client._urljoin('http://' + self.mesos_master, '#', 'agents', agent_id,
                                                      'browse?' + query_string)

            stdout_path = os.path.join(sandbox_path, 'stdout')
            params = {'path': stdout_path}
            query_string = urlencode(params)
            stdout = self._client._urljoin('http://' + agent_hostname + ':' + agent_port, 'files',
                                           'download?' + query_string)

            stderr_path = os.path.join(sandbox_path, 'stderr')
            params = {'path': stderr_path}
            query_string = urlencode(params)
            stderr = self._client._urljoin('http://' + agent_hostname + ':' + agent_port, 'files',
                                           'download?' + query_string)

        res = {'dispatcher_url': dispatcher_url, 'history_server_url': history_server_url,
               'mesos_sandbox_url': mesos_sandbox_url, 'stderr': stderr, 'stdout': stdout, }

        return res

    def logs(self, submissionId, timeout=0):
        stdout_url = self.urls(submissionId, True)['stdout']
        request = Request(stdout_url)

        start = time.time()
        end = start + timeout

        previous_chunk_size = 0
        while self.status(submissionId) == DriverState.RUNNING and (timeout <= 0 or time.time() < end):
            response = urlopen(request)
            current_chunk = response.readlines()
            for line in current_chunk[previous_chunk_size:len(current_chunk)]:
                yield line
            previous_chunk_size = len(current_chunk)
            # Carry on checking for log:
            time.sleep(5)
