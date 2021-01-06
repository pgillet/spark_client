import logging
import threading

from pymesos import OperatorMaster


class MesosState(object):
    TASK_FINISHED = 'TASK_FINISHED'
    TASK_KILLED = 'TASK_KILLED'
    TASK_FAILED = 'TASK_FAILED'
    TASK_RUNNING = 'TASK_RUNNING'
    TASK_LOST = 'TASK_LOST'
    TASK_STAGING = 'TASK_STAGING'


class PriorityHandler(object):
    EXCEPTIONAL = 20
    URGENT = 10
    ROUTINE = 0

    _clazzNames = {
        ROUTINE: 'ROUTINE',
        URGENT: 'URGENT',
        EXCEPTIONAL: 'EXCEPTIONAL',
        'ROUTINE': ROUTINE,
        'URGENT': URGENT,
        'EXCEPTIONAL': EXCEPTIONAL,
    }

    def _check_class(self, clazz):
        if isinstance(clazz, int):
            rv = self._clazzNames[clazz]
        elif str(clazz) == clazz:
            if clazz not in self._clazzNames:
                raise ValueError("Unknown priority class: {}".format(clazz))
            rv = clazz
        else:
            raise TypeError("Level not an integer or a valid string: {}".format(clazz))
        return rv

    def _check_weight(self, weight):
        if isinstance(weight, int):
            rv = weight
        elif str(weight) == weight:
            if not weight.isdigit():
                raise TypeError("No digit value: {}".format(weight))
            rv = int(weight)
            if rv < 0:
                raise ValueError("Incorrect weight value: {}".format(weight))
        else:
            raise TypeError("Weight not an integer or a valid string: {}".format(weight))
        return rv

    def _check(self, clazz, weight):
        c = self._check_class(clazz)
        w = self._check_weight(weight)
        return c, w

    def _check_priority_literal(self, priority):
        for i, c in enumerate(priority):
            if c.isdigit():
                clazz = priority[0:i]
                weight = priority[i:]
                return self._check(clazz, weight)
        raise ValueError("Unknown priority value: {}".format(priority))

    def encode(self, clazz, weight):
        clazz, weight = self._check(clazz, weight)
        if weight > 10:
            weight = 10
        return clazz + str(weight)

    def decode(self, priority):
        clazz, weight = self._check_priority_literal(priority)
        if weight > 10:
            weight = 10
        return clazz + str(weight)


class EventOperator(OperatorMaster):
    """
    An operator that can be used as a 'context guard' to define a block of code between the start and the
    completion of a Spark job in Mesos.
    """

    def __init__(self):
        self._log = logging.getLogger(__name__)
        self.events = {}

    def _register(self, submissionId):
        """
        Register an event object for the specified job: one thread signals an event related to this job, and other
        threads can wait for it.
        :param submissionId: The Spark job ID
        """
        self.events[submissionId] = threading.Event()

    def _unregister(self, submissionId):
        """
        Delete the event object associated with the specified job.
        :param submissionId: The Spark job ID
        """
        self.events.pop(submissionId, None)

    def wait_until_start(self, submissionId, timeout=None):
        """
        Block until the specified job is started in Mesos (STAGING or RUNNING)
        :param submissionId: The Spark job ID
        :param timeout: timeout for the operation in seconds
        """
        self._register(submissionId)
        self._wait(submissionId, timeout)
        # Reset the event, the next calling to wait() will block until set() is called again
        self.events[submissionId].clear()

    def wait_until_completed(self, submissionId, timeout=None):
        """
        Block until the specified job is completed (TASK_FINISHED, TASK_KILLED, TASK_FAILED or TASK_LOST)
        :param submissionId: The Spark job ID
        :param timeout: timeout for the operation in seconds
        """
        self._wait(submissionId, timeout)
        # Unregister the event object for this job
        self._unregister(submissionId)

    def _wait(self, submissionId, timeout=None):
        """
        Block until the internal flag for the given job is set to true.
        Raise a RuntimeError if timeout is not None and if the operation times out.
        :param submissionId: The Spark job ID
        :param timeout for the operation in seconds
        """
        rv = self.events[submissionId].wait(timeout)
        if not rv:
            # The operation times out
            raise RuntimeError('Job \'{}\' timed out'.format(submissionId))

    def taskAdded(self, task_info):
        """
        This method should not be called directly.
        """
        self._log.debug('Task added')
        self._log.debug(task_info)
        task_id = task_info.get('task_id')
        # Wake up the job waiting for this Mesos task to start
        if task_id:
            event = self.events.get(task_id['value'])
            if event:
                self._log.info('Task \'{}\' added'.format(task_id['value']))
                event.set()

    def taskUpdated(self, task_info):
        """
        This method should not be called directly.
        """
        self._log.debug('Task updated')
        self._log.debug(task_info)
        task_id = task_info['status']['task_id']['value']
        event = self.events.get(task_id)
        if event:
            # Wake up the job waiting for this Mesos task to finish
            state = task_info['state']
            self._log.info('Task \'{}\' is {}'.format(task_id, state))
            if state in ['TASK_FINISHED', 'TASK_KILLED', 'TASK_FAILED', 'TASK_LOST']:
                # Notify when the task is finished
                event.set()
