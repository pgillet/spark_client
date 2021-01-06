import logging

from pymesos import MesosOperatorMasterDriver


class Detector(object):
    def __init__(self, masterHost='leader.mesos', masterPort=5050):
        """
        :param masterHost: The Mesos leader's hostname or a list of Mesos masters' hostname, in which case the Mesos
        leader will be automatically determined.
        :param masterPort: The Mesos master's port
        """
        self._log = logging.getLogger(__name__)

        if type(masterHost) is list:
            for hostname in masterHost:
                try:
                    self._log.debug('Try \'{}\' as Mesos leader'.format(hostname))
                    driver = MesosOperatorMasterDriver('{}:{}'.format(hostname, masterPort))
                    driver.getHealth()
                    self.driver = driver
                    self.mesos_leader = hostname
                    break
                except Exception as ex:
                    if '307' in str(ex):
                        # HTTP Temporary Redirect
                        self._log.debug(
                            '\'{}\' is not the Mesos leader. Try next hostname in the list'.format(hostname))
                    else:
                        raise ex
            if not self.driver:
                raise ValueError('Could not determine the Mesos leader')
            self._log.debug('\'{}\' is Mesos leader'.format(hostname))
        else:
            self.mesos_leader = masterHost
            self.driver = MesosOperatorMasterDriver('{}:{}'.format(self.mesos_leader, masterPort))
            # self.driver.getHealth()

    def get_mesos_leader(self):
        """
        Returns the Mesos leader's hostname
        :return:
        """
        return self.mesos_leader

    def get_mesos_cluster_dispatcher(self):
        """
            Try to automatically detect the MesosClusterDispatcher's hostname by retrieving
            information about a framework known to the Mesos master and whose name contains 'dispatcher'
        """
        self._log.debug('Attempt to automatically detect the MesosClusterDispatcher hostname')
        hostname = None
        get_frameworks = self.driver.getFrameworks()['get_frameworks']
        frameworks = get_frameworks['frameworks']
        frameworks_list = list(
            filter(lambda x: x['active'] and 'dispatcher' in x['framework_info']['name'], frameworks))
        if len(frameworks_list) > 0:
            framework = frameworks_list[0]
            # hostname = framework['framework_info']['hostname']
            # Hostname may be the internal IP address, try webui_url instead
            hostname = framework['framework_info']['webui_url']
            start = hostname.index('//')
            end = hostname.index(':', start)
            hostname = hostname[start + 2:end]
            self._log.debug('MesosClusterDispatcher hostname detected = ' + hostname)
        else:
            raise ValueError('MesosClusterDispatcher not found')
        return hostname

    def get_spark_history_server(self):
        """
            Try to automatically detect the Spark History Server's hostname by retrieving
            information about an active task known to the Mesos master and whose name contains 'history'
        """
        self._log.debug('Attempt to automatically detect the Spark History Server hostname')
        get_state = self.driver.getState()['get_state']

        agent_id = None
        get_tasks = get_state['get_tasks']
        tasks = get_tasks['tasks']
        tasks_list = list(filter(lambda x: 'history' in x['name'], tasks))
        if len(tasks_list) > 0:
            task = tasks_list[0]
            agent_id = task['agent_id']['value']
            self._log.debug("Agent ID of Spark History Server = " + agent_id)

        hostname = None
        if agent_id:
            get_agents = get_state['get_agents']
            agents = get_agents['agents']
            agents_list = list(filter(lambda x: x['agent_info']['id']['value'] == agent_id, agents))
            if len(agents_list) > 0:
                agent = agents_list[0]
                hostname = agent['agent_info']['hostname']
                self._log.debug("Spark History Server hostname detected = " + hostname)
        else:
            raise ValueError("Spark History Server not found")

        return hostname


def test():
    logging.basicConfig(level='DEBUG', format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s')
    detector = Detector(['master-cluster-01', 'master-cluster-02', 'master-cluster-03'])
    detector.get_mesos_cluster_dispatcher()
    detector.get_spark_history_server()


if __name__ == '__main__':
    test()
