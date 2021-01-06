import json
import logging

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

TIMEOUT = 5
NB_RETRIES = 7
BACKOFF_FACTOR = 2
STATUS_FORCELIST = (500, 502, 504)


def requests_retry_session(retries=NB_RETRIES, backoff_factor=BACKOFF_FACTOR, status_forcelist=STATUS_FORCELIST,
                           session=None):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


class DeployMode(object):
    CLIENT_MODE = 'client'
    CLUSTER_MODE = 'cluster'


class DriverState(object):
    SUBMITTED = 'SUBMITTED'
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'
    RELAUNCHING = 'RELAUNCHING'
    UNKNOWN = 'UNKNOWN'
    KILLED = 'KILLED'
    FAILED = 'FAILED'
    ERROR = 'ERROR'
    NOT_FOUND = 'NOT_FOUND'


class Action(object):
    CREATE_SUBMISSION_REQUEST = 'CreateSubmissionRequest'
    CREATE_SUBMISSION_RESPONSE = 'CreateSubmissionResponse'
    KILL_SUBMISSION_RESPONSE = 'KillSubmissionResponse'
    SUBMISSION_STATUS_RESPONSE = 'SubmissionStatusResponse'


class SparkProperties(object):
    SPARK_SUBMIT_PY_FILES = 'spark.submit.pyFiles'
    SPARK_JARS = 'spark.jars'
    SPARK_MASTER = 'spark.master'
    SPARK_APP_NAME = 'spark.app.name'

    # Coarse-grained mode
    SPARK_EXECUTOR_CORES = 'spark.executor.cores'
    SPARK_EXECUTOR_MEMORY = 'spark.executor.memory'
    SPARK_CORES_MAX = 'spark.cores.max'

    # Dynamic allocation
    SPARK_DYNAMICALLOCATION_ENABLED = 'spark.dynamicAllocation.enabled'
    SPARK_SHUFFLE_SERVICE_ENABLED = 'spark.shuffle.service.enabled'
    SPARK_DYNAMICALLOCATION_INITIALEXECUTORS = 'spark.dynamicAllocation.initialExecutors'
    SPARK_DYNAMICALLOCATION_MINEXECUTORS = 'spark.dynamicAllocation.minExecutors'
    SPARK_DYNAMICALLOCATION_MAXEXECUTORS = 'spark.dynamicAllocation.maxExecutors'
    SPARK_EXECUTOR_INSTANCES = 'spark.executor.instances'

    # Scheduling
    SPARK_SCHEDULER_MODE = 'spark.scheduler.mode'

    SPARK_MESOS_ROLE = 'spark.mesos.role'

    # Executor
    SPARK_EXECUTOR_EXTRACLASSPATH = 'spark.executor.extraClassPath'
    SPARK_MESOS_EXECUTOR_HOME = 'spark.mesos.executor.home'
    SPARK_EXECUTOR_EXTRAJAVAOPTIONS = 'spark.executor.extraJavaOptions'

    # Driver
    SPARK_DRIVER_CORES = 'spark.driver.cores'
    SPARK_DRIVER_MEMORY = 'spark.driver.memory'
    SPARK_DRIVER_EXTRACLASSPATH = 'spark.driver.extraClassPath'
    SPARK_DRIVER_EXTRAJAVAOPTIONS = 'spark.driver.extraJavaOptions'

    # Docker
    SPARK_MESOS_EXECUTOR_DOCKER_IMAGE = 'spark.mesos.executor.docker.image'
    SPARK_MESOS_EXECUTOR_DOCKER_FORCEPULLIMAGE = 'spark.mesos.executor.docker.forcePullImage'
    SPARK_MESOS_EXECUTOR_DOCKER_VOLUMES = 'spark.mesos.executor.docker.volumes'
    SPARK_MESOS_EXECUTOR_DOCKER_PARAMETERS = 'spark.mesos.executor.docker.parameters'
    SPARK_MESOS_EXECUTOR_DOCKER_PORTMAPS = 'spark.mesos.executor.docker.portmaps'

    # Mesos
    SPARK_MESOS_CONSTRAINTS = 'spark.mesos.constraints'
    SPARK_MESOS_DRIVER_CONSTRAINTS = 'spark.mesos.driver.constraints'

    # Logging
    SPARK_EVENTLOG_ENABLED = 'spark.eventLog.enabled'
    SPARK_EVENTLOG_DIR = 'spark.eventLog.dir'
    SPARK_MESOS_DISPATCHER_HISTORYSERVER_URL = 'spark.mesos.dispatcher.historyServer.url'
    SPARK_MESOS_DISPATCHER_WEBUI_URL = 'spark.mesos.dispatcher.webui.url'

    def __init__(self, appName, master='mesos://spark-dispatcher.spark.marathon.mesos:7077', jars=None, pyFiles=None,
                 otherSparkProperties={}):
        """

        :param appName: A name of your application
        :param master: mesos://host:port
        :param jars: Comma-separated list of local jars to include on the driver and executor classpaths.
        :param pyFiles: Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.
        :param otherSparkProperties: Arbitrary Spark configuration properties
        :type otherSparkProperties: dict
        """
        setattr(self, SparkProperties.SPARK_APP_NAME, appName)
        # setattr(self, SparkProperties.SPARK_MASTER, master)
        if jars:
            setattr(self, SparkProperties.SPARK_JARS, jars)
        if pyFiles:
            setattr(self, SparkProperties.SPARK_SUBMIT_PY_FILES, pyFiles)
        for k, v in otherSparkProperties.items():
            setattr(self, k, v)


class JobSubmitRequest(object):
    SPARK_ENV_LOADED = 'SPARK_ENV_LOADED'

    def __init__(self, appResource, appArgs=None, clientSparkVersion='2.1.0', environmentVariables={},
                 mainClass='DUMMY', sparkProperties=None):
        """

        :param appResource: <app jar | python file>
        :param appArgs: [app arguments]
        :type appArgs: list
        :param clientSparkVersion:
        :param environmentVariables:
        :type environmentVariables: dict
        :param mainClass: Your application's main class (for Java / Scala apps)
        :param sparkProperties: Spark configuration properties
        :type sparkProperties: SparkProperties
        """
        self.action = Action.CREATE_SUBMISSION_REQUEST
        self.appArgs = appArgs
        self.appResource = appResource
        self.clientSparkVersion = clientSparkVersion
        self.environmentVariables = environmentVariables
        if self.SPARK_ENV_LOADED not in self.environmentVariables:
            self.environmentVariables[self.SPARK_ENV_LOADED] = '1'
        self.mainClass = mainClass
        self.sparkProperties = sparkProperties

    def toJSON(self):
        str = json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
        return str


class SparkClient(object):
    def __init__(self, masterHost, masterPort, clientSparkVersion):
        self.masterHost = masterHost
        self.masterPort = masterPort
        self.clientSparkVersion = clientSparkVersion
        self._masterApiRoot = 'v1/submissions'
        self._deployMode = DeployMode.CLUSTER_MODE
        self._log = logging.getLogger(__name__)

    # @property
    def master_url(self):
        return '{}:{}/{}'.format(self.masterHost, self.masterPort, self._masterApiRoot)

    def status(self, submissionId):
        """Returns the status of a Spark Application

        :param submissionId: the submission ID returned by the submit method
        :return: a str describing the driver's state
        """
        url = self._urljoin('http://' + self.master_url(), 'status', submissionId)
        r = requests_retry_session().get(url)
        r.raise_for_status()
        resContent = r.json()
        # resContent['workerHostPort'], resContent['workerId'],
        return resContent['driverState']

    def kill(self, submissionId):
        """Kill a Spark application

        :param submissionId: the submission ID returned by the submit method
        :return: a boolean value telling whether the Spark application has been successfully killed or not
        """
        url = self._urljoin('http://' + self.master_url(), 'kill', submissionId)
        r = requests_retry_session().post(url)
        r.raise_for_status()
        resContent = r.json()
        # action, message, serverSparkVersion, submissionId,
        return resContent['success']

    def submit(self, jobSubmitRequest):
        """Submit a Spark application

        :param jobSubmitRequest:
        :type jobSubmitRequest: JobSubmitRequest
        :return:
        """
        url = self._urljoin('http://' + self.master_url(), 'create')
        headers = {'Content-Type': 'application/json', 'charset': 'UTF-8'}
        json = jobSubmitRequest.toJSON()
        self._log.info(json)

        r = requests_retry_session().post(url, headers=headers, data=json)
        r.raise_for_status()
        resContent = r.json()
        self._log.debug(resContent)

        # action, message, serverSparkVersion, submissionId,
        return resContent

    def _urljoin(self, *args):
        """A custom version of urljoin that simply joins strings into a path.

        The real urljoin takes into account web semantics like when joining a url
        like /path this should be joined to http://host/path as it is an anchored
        link. We generally won't care about that in client.
        """
        return '/'.join(str(a or '').strip('/') for a in args)
