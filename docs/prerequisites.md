# Service Account for Driver Pods

Spark driver pods need a Kubernetes service account in the pod's namespace that has permissions to create, get, 
list, and delete executor pods. Please refer to [`hippi-spark-rbac.yaml`](../k8s/hippi-spark-rbac.yaml) for an
 example RBAC setup that creates a driver service account named `hippi-spark` in the namespace `spark-jobs`, with a
 RBAC role binding giving the service account the needed permissions. 

```bash
kubectl create namespace spark-jobs
kubectl create -f k8s/hippi-spark-rbac.yaml
```

# Node Affinity

You can constrain driver pods and executor pods to only be able to run on particular node(s).

Execute the following command for the node(s) intended to execute driver pods:

```bash
kubectl label nodes <node-name> type=driver
```

For executor pods:

```bash
kubectl label nodes <node-name> type=compute
```

# Pod Priority and Preemption

To use priority and preemption capabilities, create the necessary `PriorityClasses`:

```bash
kubectl create -f k8s/priorities.yaml
```
