import logging
import os

from spark_client.facade import K8S_SPARK_NATIVE, SparkJobConf
from spark_client.utils import SparkJobRunnerFactory

logging.basicConfig(level=logging.DEBUG)

appResource = "whatever"
manager = K8S_SPARK_NATIVE
job_conf = SparkJobConf(appResource=appResource, manager=manager)

# Set path of the kubeconfig file to configure cluster access
job_conf.extra_property("kubeconfig_file", os.path.join(os.path.dirname(__file__), "kubeconfig-sa"))

# Job runner
job_runner = SparkJobRunnerFactory.create_job_runner(job_conf)

# This will throw a raw kubernetes.client.exceptions.ApiException: 404 Not Found
job_runner.status("dummy")
