from __future__ import absolute_import

import binascii
import logging
import os
from os import listdir
from pprint import pformat
from random import random

import yaml
from kubernetes import config, utils, client, watch

from spark_client.interface import SparkJobRunner


class PriorityClass(object):
    ROUTINE = "routine"
    URGENT = "urgent"
    EXCEPTIONAL = "exceptional"
    RUSH = "rush"


class DriverPhase(object):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


class K8sSparkNativeJobRunner(SparkJobRunner):
    K8S_DEFAULT_DIR = "k8s/spark-native"
    DRIVER_NAME_SUFFIX = "-driver"
    DRIVER_SERVICE_NAME_SUFFIX = "-driver-svc"
    UI_INGRESS_NAME_SUFFIX = "-ui-ingress"
    UI_SERVICE_NAME_SUFFIX = "-ui-svc"

    def __init__(self, k8s_dir=None, kubeconfig_file=None, namespace="spark-jobs"):
        """
        :param k8s_dir: Path to the directory containing YAML resource definition files.
        :type k8s_dir: str
        :param kubeconfig_file: Path to the kubeconfig file for configuring access to the Kubernetes cluster.
        :type kubeconfig_file: str
        :param namespace: The Kubernetes namespace where to launch Spark applications.
        :type namespace: str
        """
        super(K8sSparkNativeJobRunner, self).__init__()
        # Precedence: kubeconfig_file > env var 'KUBECONFIG' > '~/.kube/config'
        config.load_kube_config(kubeconfig_file)

        if k8s_dir is None:
            self._k8s_dir = os.path.join(os.path.dirname(__file__), self.K8S_DEFAULT_DIR)
        else:
            self._k8s_dir = k8s_dir

        self._k8s_client = client.ApiClient()
        self._core_v1_api = client.CoreV1Api()
        self._networking_v1_beta1_api = client.NetworkingV1beta1Api()
        self._verbose = True
        self._namespace = namespace
        self._log = logging.getLogger(__name__)

    def _create_k8s_object(self, yaml_file, env_subst=None):
        with open(yaml_file) as f:
            str = f.read()
            if env_subst:
                for place_holder, value in env_subst.items():
                    str = str.replace(place_holder, value)
            return yaml.safe_load(str)

    def run(self):
        """
        :return: submission id
        """
        name_suffix = "-" + binascii.b2a_hex(os.urandom(8))
        env_subst = self._get_env_subst(name_suffix)

        # Create driver pod
        driver_pod_def_file = "spark-driver-pod.yaml"
        k8s_object_dict = self._create_k8s_object(os.path.join(self._k8s_dir, driver_pod_def_file), env_subst)
        self._apply_conf(k8s_object_dict)
        self._log.debug(pformat(k8s_object_dict))
        self._output_yaml(k8s_object_dict, driver_pod_def_file)
        k8s_objects = utils.create_from_dict(self._k8s_client, k8s_object_dict, verbose=self._verbose)

        res = k8s_objects[0].metadata.labels["app-name"]

        # Prepare ownership on dependent objects
        owner_refs = [{"apiVersion": "v1",
                       "controller": True,
                       "kind": "Pod",
                       "name": k8s_objects[0].metadata.name,
                       "uid": k8s_objects[0].metadata.uid}]

        # List all YAML files in k8s/spark-submit directory, except the driver pod definition file
        other_resources = listdir(self._k8s_dir)
        other_resources.remove(driver_pod_def_file)
        for f in other_resources:
            k8s_object_dict = self._create_k8s_object(os.path.join(self._k8s_dir, f), env_subst)
            # Set ownership
            k8s_object_dict["metadata"]["ownerReferences"] = owner_refs
            self._log.debug(pformat(k8s_object_dict))
            self._output_yaml(k8s_object_dict, f)
            utils.create_from_dict(self._k8s_client, k8s_object_dict, verbose=self._verbose)

        self._log.info("Submitted {}".format(res))
        return res

    def _get_env_subst(self, name_suffix):
        # Default values
        env_subst = {"${DRIVER_IMAGE}": "eu.gcr.io/hippi-spark-k8s/spark-py:3.0.1",
                     "${DRIVER_NODE_AFFINITIES}": "driver",
                     "${EXECUTOR_NODE_AFFINITIES}": "compute",
                     "${NAME_SUFFIX}": name_suffix,
                     "${PRIORITY_CLASS_NAME}": PriorityClass.ROUTINE, }

        # Add or override with extra properties
        for k, v in self.job_conf.otherProperties.items():
            env_subst["${{{}}}".format(k)] = v

        return env_subst

    def _apply_conf(self, k8s_object_dict):
        container = k8s_object_dict["spec"]["containers"][0]

        # env
        env_vars = container["env"]
        for k, v in self.job_conf.environmentVariables.items():
            env_vars += [{"name": k, "value": v}]

        # args
        args = ["$(SPARK_HOME)/bin/spark-submit"]
        if self.job_conf.mainClass is not None:
            args += ["--class", self.job_conf.mainClass]
        args += ["--name", self.job_conf.appName]
        for k, v in self.job_conf.sparkProperties.items():
            args += ["--conf", "{}={}".format(k, v)]
        args += [self.job_conf.appResource]
        for arg in self.job_conf.appArgs:
            args += [arg]
        container["args"] = args

    def _output_yaml(self, k8s_object_dict, filename):
        if "output_dir" in self.job_conf.otherProperties:
            output_file_path = os.path.join(self.job_conf.otherProperties["output_dir"], filename)
            with open(output_file_path, "w") as output_file:
                yaml.dump(k8s_object_dict, output_file, default_flow_style=False)

    def kill(self, app_name):
        label_selector = "app-name={},spark-role=driver".format(app_name)
        ret = self._core_v1_api.list_namespaced_pod(namespace=self._namespace, label_selector=label_selector)
        if (len(ret.items)) > 1:
            raise ValueError("Spark app name should be unique")
        if ret.items:
            pod = ret.items[0]
            self._core_v1_api.delete_namespaced_pod(pod.metadata.name, self._namespace, propagation_policy="Background")

            # Due to ownership relationships, all dependent objects are deleted in cascade and in the background
            self._log.info("Deleted Spark application {}".format(app_name))
        else:
            raise ValueError("Spark app {} not found".format(app_name))

    def status(self, app_name):
        pod_name = app_name + K8sSparkNativeJobRunner.DRIVER_NAME_SUFFIX
        pod_status = self._core_v1_api.read_namespaced_pod_status(pod_name, self._namespace)  # Cannot be watched
        self._log.info("Pod phase: {}".format(pod_status.status.phase))
        return pod_status.status.phase

    def is_finished(self, app_name):
        """Returns True if the specified job has succeeded, False otherwise."""
        return self.status(app_name) == DriverPhase.SUCCEEDED

    def is_killed(self, submissionId):
        # We cannot tell the difference between a pod that never existed and a killed pod: a killed pod is simply
        # deleted and and it no longer has a status, by definition.
        pass

    def is_running(self, app_name):
        return self.status(app_name) == DriverPhase.RUNNING

    def is_queued(self, submissionId):
        # A Pod has a PodStatus, which has an array of PodConditions through which the Pod has or has not passed,
        # among which:
        # PodScheduled: the Pod has been scheduled to a node. This means the pod is about to start.
        # We don't really have a way of knowing where the pod is between the moment a job is submitted and the moment
        # the job starts, and even if it exists ...
        # So, this method does not make sense.
        pass

    def is_not_queued(self, submissionId):
        pass

    def is_completed(self, app_name):
        """Returns True if the specified job is in one of the final states (Succeeded, Failed, Unknown?),
        False otherwise.
        """
        return self._has_state(app_name, [DriverPhase.SUCCEEDED, DriverPhase.FAILED, ])  # DriverState.UNKNOWN ?

    def _has_state(self, submissionId, states):
        """
        Returns True if the specified job is in one of the given states, False otherwise.
        """
        status = self.status(submissionId)
        self._log.debug('Current state of job {}: {}'.format(submissionId, status))
        return status in states

    def _wait_until_phase(self, app_name, expected_phases, timeout=0, period=5):
        """This method will block until the pod driver is in one of the expected phases.
        :type expected_phases: Set
        """
        timeout_seconds = timeout if timeout > 0 else 86400
        w = watch.Watch()
        label_selector = "app-name={},spark-role=driver".format(app_name)
        for event in w.stream(self._core_v1_api.list_namespaced_pod, namespace=self._namespace,
                              label_selector=label_selector,
                              timeout_seconds=timeout_seconds):
            phase = event['object'].status.phase
            self._log.debug("Event: {}".format(phase))

            if phase in expected_phases:
                w.stop()

        self._log.info("Job {} is {}".format(app_name, phase))

    def wait_until_completed(self, app_name, timeout=0, period=5):
        # DriverPhase.UNKNOWN: "For some reason the state of the Pod could not be obtained. This phase typically occurs
        # due to an error in communicating with the node where the Pod should be running."
        # Thus, DriverPhase.UNKNOWN is not a terminal state.
        self._wait_until_phase(app_name, [DriverPhase.SUCCEEDED, DriverPhase.FAILED], timeout=timeout, period=period)

    def wait_until_start(self, app_name, timeout=0, period=5):
        self._wait_until_phase(app_name, [DriverPhase.RUNNING], timeout=timeout, period=period)

    def _get_node_port_url(self, app_name):
        ui_service_name = app_name + K8sSparkNativeJobRunner.UI_SERVICE_NAME_SUFFIX

        ui_svc = self._core_v1_api.read_namespaced_service(name=ui_service_name, namespace=self._namespace)
        node_port = ui_svc.spec.ports[0].node_port

        # Choose a random node
        nodes = self._core_v1_api.list_node()
        n = random.randint(0, len(nodes.items) - 1)
        node = nodes.items[n]

        external_ip = filter(lambda addr: addr.type == "ExternalIP", node.status.addresses)[0].address
        node_port_url = "http://{}:{}".format(external_ip, node_port)
        self._log.info(
            "The Spark Web UI for {} is available at {} (NodePort)".format(app_name, node_port_url))
        return node_port_url

    def _get_ingress_url(self, app_name):
        ingress_url = None
        ui_ingress_name = app_name + K8sSparkNativeJobRunner.UI_INGRESS_NAME_SUFFIX
        ui_ingress_status = self._networking_v1_beta1_api.read_namespaced_ingress_status(name=ui_ingress_name,
                                                                                         namespace=self._namespace)
        ingress = ui_ingress_status.status.load_balancer.ingress
        if ingress:
            external_ip = ingress[0].ip
            ingress_url = "http://{}/{}".format(external_ip, app_name)
            self._log.info(
                "The Spark Web UI for {} is available at {} (Ingress)".format(app_name, ingress_url))
        else:
            self._log.info("Ingress for {} not yet available".format(app_name))

        return ingress_url

    def _watch_ingress_url(self, app_name, timeout=0):
        ingress_url = None
        timeout_seconds = timeout if timeout > 0 else 86400
        w = watch.Watch()
        label_selector = "app-name={}".format(app_name)
        for event in w.stream(self._networking_v1_beta1_api.list_namespaced_ingress, namespace=self._namespace,
                              label_selector=label_selector,
                              timeout_seconds=timeout_seconds):
            ingress = event['object'].status.load_balancer.ingress
            if ingress:
                external_ip = ingress[0].ip
                ingress_url = "http://{}/{}".format(external_ip, app_name)
                self._log.info("Event: The Spark Web UI for {} is available at {}".format(app_name, ingress_url))
                w.stop()
            else:
                self._log.info("Event: Ingress for {} not yet available".format(app_name))

        return ingress_url

    def urls(self, app_name, watch=False, timeout=0, period=5):
        # node_port_url = self._get_node_port_url(app_name)

        if watch:
            ingress_url = self._watch_ingress_url(app_name, timeout=timeout)
        else:
            ingress_url = self._get_ingress_url(app_name)

        res = {
            # 'node_port_url': node_port_url,
            'ingress_url': ingress_url, }

        return res

    def logs(self, app_name, timeout=0):
        timeout_seconds = timeout if timeout > 0 else 86400
        pod_name = app_name + K8sSparkNativeJobRunner.DRIVER_NAME_SUFFIX
        w = watch.Watch()
        for event in w.stream(self._core_v1_api.read_namespaced_pod_log, pod_name, self._namespace,
                              _request_timeout=timeout_seconds):
            yield event


class K8sSparkOperatorJobRunner(K8sSparkNativeJobRunner):
    K8S_DEFAULT_DIR = "k8s/spark-operator"
    GROUP = "sparkoperator.k8s.io"
    VERSION = "v1beta2"
    KIND = "SparkApplication"
    PLURAL = "sparkapplications"

    # A dict that maps Spark properties to yaml "paths" in SparkApplication custom resource.
    # Properties below are related to deployment, typically set through configuration file or spark-submit command line
    # options. These properties will not be applied if passed directly to '.spec.sparkConf' in the SparkApplication
    # custom resource. Indeed, '.spec.sparkConf' is only intended for properties that affect Spark runtime control,
    # like "spark.task.maxFailures".
    # Also pay attention to properties that specify limits or amount of resources to request at Spark-level vs
    # Pod-level: '.spec.driver.memory' is not 'spark.driver.memory'.
    # TODO: manage special properties 'spark.submit.pyFiles' and 'spark.jars' ?
    # But these properties are multi-valued. See https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/api-docs.md#sparkoperator.k8s.io/v1beta2.Dependencies
    DEPLOY_PROPERTIES = {
        "spark.kubernetes.driver.request.cores": "spec.driver.coreRequest",
        "spark.kubernetes.executor.request.cores": "spec.executor.coreRequest",
        "spark.kubernetes.executor.deleteOnTermination": "spec.executor.deleteOnTermination",
        "spark.driver.cores": "spec.driver.cores",
        "spark.executor.cores": "spec.executor.cores",
        "spark.executor.instances": "spec.executor.instances",
        "spark.kubernetes.container.image": "spec.image",
        "spark.kubernetes.driver.container.image": "spec.driver.image",
        "spark.kubernetes.executor.container.image": "spec.executor.image",
        "spark.kubernetes.container.image.pullPolicy": "spec.imagePullPolicy",
    }

    # A dict that maps Spark properties to their expected type of value.
    # There is no need to map Spark properties that expect string values.
    DEPLOY_PROPERTIES_TYPE = {
        "spark.driver.cores": int,
        "spark.executor.cores": int,
        "spark.executor.instances": int,
    }

    def __init__(self, k8s_dir=None, kubeconfig_file=None, namespace="spark-jobs"):
        super(K8sSparkOperatorJobRunner, self).__init__(k8s_dir, kubeconfig_file, namespace)
        self._custom_object_api = client.CustomObjectsApi()

    def run(self):
        name_suffix = "-" + binascii.b2a_hex(os.urandom(8))
        env_subst = self._get_env_subst(name_suffix)

        # Create pod
        yaml_file = os.path.join(self._k8s_dir, "spark.yaml")
        spark_app = self._create_k8s_object(yaml_file, env_subst)
        self._apply_conf(spark_app)
        self._log.debug(pformat(spark_app))
        self._output_yaml(spark_app, "spark.yaml")

        # create the resource
        self._custom_object_api.create_namespaced_custom_object(
            group=K8sSparkOperatorJobRunner.GROUP,
            version=K8sSparkOperatorJobRunner.VERSION,
            namespace=self._namespace,
            plural=K8sSparkOperatorJobRunner.PLURAL,
            body=spark_app,
        )
        self._log.info("Resource created")

        # get the resource and print out data
        resource = self._custom_object_api.get_namespaced_custom_object(
            group=K8sSparkOperatorJobRunner.GROUP,
            version=K8sSparkOperatorJobRunner.VERSION,
            name="spark-{}{}".format(self.job_conf.otherProperties.get("PRIORITY_CLASS_NAME", PriorityClass.ROUTINE),
                                     name_suffix),
            namespace=self._namespace,
            plural=K8sSparkOperatorJobRunner.PLURAL,
        )
        self._log.debug("Resource details:")
        self._log.debug(pformat(resource))

        app_name = resource["metadata"]["name"]

        # Hijack the auto-created UI service and change its type from ClusterIP to NodePort
        # ui_service_name = app_name + K8sSparkNativeJobRunner.UI_SERVICE_NAME_SUFFIX
        #
        # w = watch.Watch()
        # field_selector = "metadata.name=%s" % ui_service_name
        # for event in w.stream(self._core_v1_api.list_namespaced_service, namespace=self._namespace,
        #                       field_selector=field_selector,
        #                       timeout_seconds=30):
        #     ui_svc = event['object']
        #     if ui_svc:
        #         w.stop()
        #     else:
        #         self._log.debug("Event: UI service not yet available")
        #
        # ui_svc.spec.type = "NodePort"
        # self._core_v1_api.patch_namespaced_service(name=ui_service_name, namespace=self._namespace, body=ui_svc)

        # Create ingress
        # Prepare ownership on dependent objects
        owner_refs = [{"apiVersion": "{}/{}".format(K8sSparkOperatorJobRunner.GROUP, K8sSparkOperatorJobRunner.VERSION),
                       "controller": True,
                       "kind": K8sSparkOperatorJobRunner.KIND,
                       "name": app_name,
                       "uid": resource["metadata"]["uid"]}]

        yaml_file = os.path.join(self._k8s_dir, "spark-ui-ingress.yaml")
        k8s_object_dict = self._create_k8s_object(yaml_file, env_subst)
        # Set ownership
        k8s_object_dict["metadata"]["ownerReferences"] = owner_refs
        self._log.debug(pformat(k8s_object_dict))
        self._output_yaml(k8s_object_dict, "spark-ui-ingress.yaml")
        utils.create_from_dict(self._k8s_client, k8s_object_dict, verbose=self._verbose)

        return app_name

    def _apply_conf(self, k8s_object_dict):
        # Deploy properties
        for k, v in self.job_conf.sparkProperties.items():
            if k in K8sSparkOperatorJobRunner.DEPLOY_PROPERTIES:
                yaml_path = K8sSparkOperatorJobRunner.DEPLOY_PROPERTIES[k]
                path_elems = yaml_path.split(".")
                yaml_elem = k8s_object_dict
                for path_elem in path_elems[:-1]:
                    if path_elem not in yaml_elem:
                        yaml_elem[path_elem] = {}
                    yaml_elem = yaml_elem[path_elem]
                yaml_elem[path_elems[-1]] = K8sSparkOperatorJobRunner.DEPLOY_PROPERTIES_TYPE.get(k, str)(v)

        spec = k8s_object_dict["spec"]

        # Spark properties
        spark_conf = spec.get("sparkConf", {})
        spark_conf.update(self.job_conf.sparkProperties)
        spec["sparkConf"] = spark_conf

        # Env vars
        for elem in ["driver", "executor"]:
            env = spec[elem].get("env", [])
            for k, v in self.job_conf.environmentVariables.items():
                env += [{"name": k, "value": v}]
            spec[elem]["env"] = env

        # App properties
        if self.job_conf.mainClass is not None:
            spec["mainClass"] = self.job_conf.mainClass
        spec["mainApplicationFile"] = self.job_conf.appResource
        spec["arguments"] = self.job_conf.appArgs

    def kill(self, app_name):
        self._custom_object_api.delete_namespaced_custom_object(
            group=K8sSparkOperatorJobRunner.GROUP,
            version=K8sSparkOperatorJobRunner.VERSION,
            namespace=self._namespace,
            plural=K8sSparkOperatorJobRunner.PLURAL,
            name=app_name,
            propagation_policy="Background")

        # Due to ownership relationships, all dependent objects are deleted in cascade and in the background
        self._log.info("Deleted Spark application {}".format(app_name))
