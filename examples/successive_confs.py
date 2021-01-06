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

# Run a 1st job
submission_id = job_runner.run()
print("Submitted job with ID = {}".format(submission_id))

# Change runtime properties
job_conf.appArgs = ["5"]
job_conf \
    .env_var("GROK", "grok") \
    .spark_property("spark.executor.instances", "3") \
    .extra_property("PRIORITY_CLASS_NAME", "urgent")

# Check configuration
print(job_conf)

# A new configuration can be applied to the same SparkJobRunner instance
job_runner.job_conf = job_conf

# Run a 2nd job with the modified configuration
submission_id = job_runner.run()
print("Submitted job with ID = {}".format(submission_id))
