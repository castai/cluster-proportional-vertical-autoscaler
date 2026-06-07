# cluster-proportional-vertical-autoscaler (BETA)

[![Build Status](https://travis-ci.org/kubernetes-sigs/cluster-proportional-vertical-autoscaler.svg)](https://travis-ci.org/kubernetes-sigs/cluster-proportional-vertical-autoscaler)
[![Go Report Card](https://goreportcard.com/badge/github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler)](https://goreportcard.com/report/github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler)

## Overview

This container image watches over the number of nodes and cores of the cluster and resizes
the resource limits and requests for a DaemonSet, ReplicaSet, or Deployment. This functionality 
may be desirable for applications where resources such as cpu and memory for a particular job need 
to be autoscaled with the size of the cluster.

Usage of cluster-proportional-vertical-autoscaler:

```
      --alsologtostderr[=false]: log to standard error as well as files
      --config-file: The default configuration (in JSON format).
      --default-config: A config file (in JSON format), which overrides the --default-config.
      --kube-config="": Path to a kubeconfig. Only required if running out-of-cluster.
      --log-backtrace-at=:0: when logging hits line file:N, emit a stack trace
      --log-dir="": If non-empty, write log files in this directory
      --logtostderr[=false]: log to standard error instead of files
      --namespace="": The Namespace of the --target. Defaults to ${MY_NAMESPACE}.
      --poll-period-seconds=10: The period, in seconds, to poll cluster size and perform autoscaling.
      --stderrthreshold=2: logs at or above this threshold go to stderr
      --target="": Target to scale. In format: deployment/*, replicaset/* or daemonset/* (not case sensitive).
      --v=0: log level for V logs
      --version[=false]: Print the version and exit.
      --vmodule=: comma-separated list of pattern=N settings for file-filtered logging
```

## Examples

Please try out the examples in [the examples folder](examples/README.md).

## Implementation Details

The code in this module is a Kubernetes Golang API client that, using the default service account credentials
available to Golang clients running inside pods, it connects to the API server and polls for the number of nodes
and cores in the cluster.

The scaling parameters and data points are provided via a config file in JSON format to the autoscaler and it 
refreshes its parameters table every poll interval to be up to date with the latest desired scaling parameters.

### Calculation of resource requests and limits

The resource requests and limits are computed by using the number of cores and nodes as input as well as
the provided step values bounded by provided base and max values.

Example:

```
Base = 10
Max = 100
Step = 2
CoresPerStep = 4
NodesPerStep = 2

The core and node counts are rounded up to the next whole step.

If we find 64 cores and 4 nodes we get scalars of:
  by-cores: 10 + (2 * (round(64, 4)/4)) = 10 + 32 = 42
  by-nodes: 10 + (2 * (round(4, 2)/2)) = 10 + 4 = 14
  
The larger is by-cores, and it is less than Max, so the final value is 42.

If we find 3 cores and 3 nodes we get scalars of:
  by-cores: 10 + (2 * (round(3, 4)/4)) = 10 + 2 = 12
  by-nodes: 10 + (2 * (round(3, 2)/2)) = 10 + 4 = 14
```

## Config parameters

The configuration should be in JSON format and supports the following parameters:
  - **base** The baseline quantity required.
  - **max**  The maximum allowed quantity.
  - **step** The amount of additional resources to grow by.  If this is too fine-grained, the resizing action will happen too frequently.
  - **coresPerStep** The number of cores required to trigger an increase.
  - **nodesPerStep** The number of nodes required to trigger an increase.
      
Example:

```
"containerA": {
  "requests": {
    "cpu": {
      "base": "10m", "step":"1m", "coresPerStep":1
    },
    "memory": {
      "base": "8Mi", "step":"1Mi", "coresPerStep":1
    }
  }
"containerB": {
  "requests": {
    "cpu": {
      "base": "250m", "step":"100m", "coresPerStep":10
    },
  }
}
```

## Running the cluster-proportional-vertical-autoscaler
This repo includes an example yaml files in the "examples" directory that can be used as examples demonstrating 
how to use the vertical autoscaler.

For example, consider a Deployment that needs to scale its resources (cpu, memory, etc...) proportional to the number of
cores in a cluster.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: thing
  namespace: kube-system
  labels:
    k8s-app: thing
spec:
  replicas: 3
  selector:
    matchLabels:
      k8s-app: thing
  template:
    metadata:
      labels:
        k8s-app: thing
    spec:
      containers:
      - image: nginx
        name: thing
```

```bash
kubectl create -f thing.yaml
```


The below config will scale the above defined deployment's CPU resource by "100m" step size
for every 10 nodes that are added to the cluster.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: thing-autoscaler
  namespace: kube-system
  labels:
    k8s-app: thing-autoscaler
    kubernetes.io/cluster-service: "true"
    addonmanager.kubernetes.io/mode: Reconcile
spec:
  selector:
    matchLabels:
      k8s-app: thing-autoscaler
  template:
    metadata:
      labels:
        k8s-app: thing-autoscaler
      annotations:
        scheduler.alpha.kubernetes.io/critical-pod: ''
    spec:
      containers:
      - name: autoscaler
        image: registry.k8s.io/cpvpa-amd64:v0.8.1
        resources:
          requests:
            cpu: "20m"
            memory: "10Mi"
        command:
          - /cpvpa
          - --target=deployment/thing
          - --namespace=kube-system
          - --logtostderr=true
          - --poll-period-seconds=10
          - --default-config={"thing":{"requests":{"cpu":{"base":"250m","step":"100m","nodesPerStep":10}}}}
      tolerations:
      - key: "CriticalAddonsOnly"
        operator: "Exists"
      serviceAccountName: thing-autoscaler
```

## In-place pod resize (Kubernetes 1.33+)

By default, cpvpa only patches the workload template; new pods come up at the correct size, but existing pods are rolled only when the owning controller recreates them. In-place resize mode uses the `pods/resize` subresource (KEP-1287) to patch live pods without restart.

### Requirements

- Kubernetes 1.33+ with the `InPlacePodVerticalScaling` feature gate enabled (on by default in 1.33).
- Each container that should resize in-place must declare a `resizePolicy` with `restartPolicy: NotRequired` for the resources you want to resize.

### RBAC

In-place modes need extra permissions beyond the base role. Apply `examples/RBAC/RBAC-inplace-configs.yaml` in addition to the base role:

```bash
kubectl apply -f examples/RBAC/RBAC-inplace-configs.yaml
```

This grants:
- `pods`: `[list, get]` — to find and classify pods owned by the target
- `pods/resize`: `[patch]` — the actual in-place resize subresource
- `pods`: `[delete]` — fallback eviction in `InPlaceOrRecreate` mode

### Resize modes

- `--resize-mode=Recreate` (default): patch only the template; existing pods are rolled by the controller.
- `--resize-mode=InPlace`: patch template + live pods via `/resize`. Pods that report `Deferred` or `Infeasible` are retried on the next poll.
- `--resize-mode=InPlaceOrRecreate`: like `InPlace`, but pods that fail to resize (kubelet reports `Infeasible` or `Deferred`, or the resize stays in progress) for longer than `--resize-fallback-grace-period` are recreated at the new size. How they're recreated depends on the target: for Deployments and `RollingUpdate` DaemonSets cpvpa patches the template and lets the controller perform its normal rolling update — which recreates **all** of the workload's pods, paced by `maxUnavailable`/PodDisruptionBudgets, not just the stuck ones; for bare ReplicaSets and `OnDelete` DaemonSets cpvpa deletes the stuck pods directly so the controller recreates them.

Both `InPlace` and `InPlaceOrRecreate` need the in-place RBAC add-on.

### Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--resize-mode` | How to apply resource changes: `Recreate`, `InPlace`, or `InPlaceOrRecreate` | `Recreate` |
| `--resize-fallback-grace-period` | Only for `InPlaceOrRecreate`. How long a pod must fail to resize (Infeasible, Deferred, or stuck in progress) before cpvpa recreates it. | `5m` |
| `--resize-fallback-max-pods-per-cycle` | Only for `InPlaceOrRecreate`. Caps how many stuck pods cpvpa deletes directly per cycle (bare ReplicaSet / OnDelete DaemonSet only; self-healing controllers pace their own rollout). | `1` |

### Example

See `examples/cpvpa-inplace-example.yaml` for a complete example including:
- a target Deployment with `resizePolicy` on its containers
- a cpvpa Deployment running in `InPlaceOrRecreate` mode
- the required RBAC bindings
