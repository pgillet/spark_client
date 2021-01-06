from spark_client.facade import MESOS, K8S_SPARK_NATIVE, K8S_SPARK_OPERATOR
from spark_client.kubernetes.spark_job_runner import K8sSparkNativeJobRunner, K8sSparkOperatorJobRunner
from spark_client.mesos.spark_job_runner import MesosSparkJobRunner


class SparkJobRunnerFactory(object):

    @staticmethod
    def create_job_runner(job_conf):
        """
        :type job_conf: SparkJobConf
        """
        if job_conf.manager == MESOS:
            mesos_master = job_conf.otherProperties.get("mesos.master", MesosSparkJobRunner.DEFAULT_MESOS_MASTER)
            spark_master = job_conf.sparkProperties.get("spark.master", MesosSparkJobRunner.DEFAULT_MASTER)
            client_spark_version = job_conf.otherProperties.get("clientSparkVersion", MesosSparkJobRunner.SPARK_VERSION)
            job_runner = MesosSparkJobRunner(mesos_master, spark_master, client_spark_version)
        else:
            k8s_dir = job_conf.otherProperties.get("k8s_dir")
            kubeconfig_file = job_conf.otherProperties.get("kubeconfig_file")
            if job_conf.manager == K8S_SPARK_NATIVE:
                job_runner = K8sSparkNativeJobRunner(k8s_dir, kubeconfig_file)
            elif job_conf.manager == K8S_SPARK_OPERATOR:
                job_runner = K8sSparkOperatorJobRunner(k8s_dir, kubeconfig_file)

        job_runner.job_conf = job_conf
        return job_runner
