from unittest import TestCase

from spark_client.facade import SparkJobConf
from spark_client.mesos.spark_job_runner import MesosSparkJobRunner


class TestMesosSparkJobRunner(TestCase):
    def test_job_conf(self):
        job_runner = MesosSparkJobRunner()

        appName = "my-app"
        appResource = "path/to/main.py"
        appArgs = ["1", "2", "3"]
        manager = "mesos"
        job_conf = SparkJobConf(appName, appResource, appArgs, manager=manager)
        job_conf.env_var("GRUIK", "gruik") \
            .env_var("GROK", "grok") \
            .spark_property("spark.executor.instances", "3") \
            .spark_property("spark.executor.memory", "4096M") \
            .extra_property("custom", "something")

        job_runner.job_conf = job_conf

        self.assertIn("GRUIK", job_runner.job_conf.environmentVariables)
        self.assertIn("GROK", job_runner.job_conf.environmentVariables)
        self.assertIn("custom", job_runner.job_conf.otherProperties)
        self.assertIn("spark.driver.cores", job_runner.job_conf.sparkProperties)  # Root conf
        self.assertIn("spark.executor.instances", job_runner.job_conf.sparkProperties)
        self.assertEqual(job_runner.job_conf.sparkProperties["spark.executor.memory"],
                         "4096M")  # Property should be overidden
