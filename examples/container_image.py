import logging
import os
from threading import Thread

from spark_client.facade import K8S_SPARK_NATIVE, SparkJobConf
from spark_client.utils import SparkJobRunnerFactory

logging.basicConfig(level=logging.DEBUG)

# Job configuration set programmatically
appName = "pyspark-pi"
appResource = "local:/opt/spark/examples/src/main/python/pi.py"
appArgs = ["2"]
manager = K8S_SPARK_NATIVE

job_conf = SparkJobConf(appName=appName, appResource=appResource, appArgs=appArgs, manager=manager)

# Set/override runtime properties
job_conf \
    .spark_property("spark.kubernetes.executor.container.image", "eu.gcr.io/hippi-spark-k8s/spark-py:3.0.1") \
    .extra_property("DRIVER_IMAGE", "eu.gcr.io/hippi-spark-k8s/spark-py:3.0.1")

# Job runner
job_runner = SparkJobRunnerFactory.create_job_runner(job_conf)

submission_id = job_runner.run()
print("Submitted job with ID = {}".format(submission_id))