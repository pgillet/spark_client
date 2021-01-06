import logging
import os
from threading import Thread

from spark_client.facade import K8S_SPARK_NATIVE, SparkJobConf
from spark_client.utils import SparkJobRunnerFactory

logging.basicConfig(level=logging.DEBUG)

# Job configuration
# Path to the static configuration file
config_path = os.path.join(os.path.dirname(__file__), "conf.cfg")
appArgs = ["5"]

job_conf = SparkJobConf(appArgs=appArgs, configPath=config_path)

# Set/override runtime properties
job_conf \
    .env_var("GROK", "grok") \
    .spark_property("spark.executor.instances", "3") \
    .extra_property("PRIORITY_CLASS_NAME", "urgent")

# Check configuration
print(job_conf)

# Job runner
job_runner = SparkJobRunnerFactory.create_job_runner(job_conf)

submission_id = job_runner.run()
print("Submitted job with ID = {}".format(submission_id))

job_runner.wait_until_start(submission_id)

status = job_runner.status(submission_id)
print("Job {} is {}".format(submission_id, status))


def consumer():
    for log in job_runner.logs(submission_id):
        print(log)


t = Thread(target=consumer)
t.start()

urls = job_runner.urls(submission_id, watch=True)
print("The Spark Web UI is available at = {}".format(urls["ingress_url"]))

job_runner.wait_until_completed(submission_id)
t.join()

status = job_runner.status(submission_id)
print("Job {} is {}".format(submission_id, status))
