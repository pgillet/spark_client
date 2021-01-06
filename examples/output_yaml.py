import logging
import os

from spark_client.facade import K8S_SPARK_NATIVE, SparkJobConf
from spark_client.utils import SparkJobRunnerFactory

logging.basicConfig(level=logging.DEBUG)

# Job configuration set programmatically
appName = "pyspark-pi"
appResource = "local:/opt/spark/examples/src/main/python/pi.py"
appArgs = ["2"]
manager = K8S_SPARK_NATIVE

job_conf = SparkJobConf(appName=appName, appResource=appResource, appArgs=appArgs, manager=manager)

# Set path of the kubeconfig file to configure cluster access
job_conf.extra_property("kubeconfig_file", os.path.join(os.path.dirname(__file__), "kubeconfig-sa"))

# Set output directory where to dump yaml files
output_dir = os.path.join(os.path.dirname(__file__), "output")
job_conf.extra_property("output_dir", output_dir)
os.mkdir(output_dir)

# Job runner
job_runner = SparkJobRunnerFactory.create_job_runner(job_conf)

submission_id = job_runner.run()
print("Submitted job with ID = {}".format(submission_id))
