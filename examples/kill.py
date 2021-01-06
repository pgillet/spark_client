import logging
import os

from spark_client.facade import K8S_SPARK_NATIVE, SparkJobConf
from spark_client.utils import SparkJobRunnerFactory

logging.basicConfig(level=logging.DEBUG)

# Job configuration
# Path to the static configuration file
config_path = os.path.join(os.path.dirname(__file__), "conf.cfg")
manager = K8S_SPARK_NATIVE  # Can also be directly set in the configuration file, in 'application' section

job_conf = SparkJobConf(manager=manager, configPath=config_path)

# Job runner
job_runner = SparkJobRunnerFactory.create_job_runner(job_conf)

submission_id = job_runner.run()
print("Submitted job with ID = {}".format(submission_id))

job_runner.wait_until_start(submission_id)

status = job_runner.status(submission_id)
print("Job {} is {}".format(submission_id, status))

job_runner.kill(submission_id)
