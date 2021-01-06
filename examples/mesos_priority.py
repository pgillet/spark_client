import logging
import os

from spark_client.facade import SparkJobConf, MESOS
from spark_client.utils import SparkJobRunnerFactory

logging.basicConfig(level=logging.DEBUG)

# Job configuration
# Path to the static configuration file
config_path = os.path.join(os.path.dirname(__file__), "conf.cfg")
manager = MESOS  # Can also be directly set in the configuration file, in 'application' section

job_conf = SparkJobConf(manager=manager, configPath=config_path)

priority = "URGENT"
job_conf \
    .spark_property("spark.mesos.role", priority) \
    .spark_property("spark.mesos.dispatcher.queue", priority)

# Check configuration
print(job_conf)

# Job runner
job_runner = SparkJobRunnerFactory.create_job_runner(job_conf)

submission_id = job_runner.run()
print("Submitted job with ID = {}".format(submission_id))
