from threading import Thread


class SparkJobRunner(object):

    def __init__(self):
        self._job_conf = None

    @property
    def job_conf(self):
        return self._job_conf

    @job_conf.setter
    def job_conf(self, value):
        """
        Set a new configuration for this SparkJobRunner.
            :param value: A L{SparkJobConf} object setting runtime properties
            :type value: SparkJobConf
        """
        self._job_conf = value

    @job_conf.deleter
    def job_conf(self):
        del self._job_conf

    def run(self):
        """"""

    def kill(self, submissionId):
        """"""

    def status(self, submissionId):
        """"""

    def is_finished(self, submissionId):
        """"""

    def is_killed(self, submissionId):
        """"""

    def is_running(self, submissionId):
        """"""

    def is_queued(self, submissionId):
        """"""

    def _has_state(self, submissionId, states):
        """Returns True if the specified job is in one of the given states, False otherwise."""

    def is_completed(self, submissionId):
        """Returns True if the specified job is completed, False otherwise."""

    def is_not_queued(self, submissionId):
        """Returns True if the specified job is not queued, False otherwise."""

    def wait_until_completed(self, submissionId, timeout=0, period=5):
        """Wait until the specified job is completed, or the timeout is up (infinite by default).

        :param submissionId: The Spark job ID
        :param timeout: Timeout in seconds. Infinite if less than or equal to zero.
        :type timeout: int
        :param period: The period of time in seconds that must elapse between each check of the job's status
        :type period: int
        :return: True if the job has completed, False otherwise
        """

    def wait_until_start(self, submissionId, timeout=0, period=5):
        """Wait until the specified job is started, or the timeout is up (infinite by default).

        :param submissionId: The Spark job ID
        :param timeout: Timeout in seconds. Infinite if less than or equal to zero.
        :type timeout: int
        :param period: The period of time in seconds that must elapse between each check of the job's status
        :type period: int
        :return: True if the jab has started, False otherwise
        """

    def urls(self, submissionId, watch=False, timeout=0, period=5):
        """Returns a dict containing different URLs for monitoring the specified job.

        :param submissionId: The Spark job ID
        :param watch: Tells whether to watch and wait until the requested resources are available.
        :type watch: bool
        :param timeout: Timeout in seconds if watch mode. Infinite if less than or equal to zero.
        :type timeout: int
        :param period: The period of time in seconds that must elapse between each request for resources, if watch mode.
        :type period: int
        :return: a dict
        """

    def logs(self, submissionId, timeout=0):
        """Read log of the driver for the specified Spark application.
        Sample usage:
        >>> for log in logs(submissionId):
        >>>     print(log)
        or
        >>> iter = logs(submissionId)
        >>> while True:
        >>>     try:
        >>>         print(next(iter))
        >>>     except StopIteration:
        >>>         break
        or if you don't want to block the main thread while reading the logs:
        >>> def consumer():
        >>>     for log in logs(submissionId):
        >>>         print(log)

        >>> t = Thread(target=consumer)
        >>> t.start()

        :param submissionId: The Spark job ID
        :param timeout: Timeout in seconds. Infinite if less than or equal to zero.
        :type timeout: int
        """
