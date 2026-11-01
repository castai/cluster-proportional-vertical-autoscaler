# cluster-proportional-vertical-autoscaler (BETA)

[![Build Status](https://travis-ci.org/kubernetes-sigs/cluster-proportional-vertical-autoscaler.svg)](https://travis-ci.org/kubernetes-sigs/cluster-proportional-vertical-autoscaler)
[![Go Report Card](https://goreportcard.com/badge/github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler)](https://goreportcard.com/report/github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler)

## Overview

This container image watches over the number of nodes and cores of the cluster and resizes
the resource limits and requests for a DaemonSet, ReplicaSet, StatefulSet, or Deployment. This functionality 
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
      --poll-period-seconds=10: The period, in seconds, to poll cluster size and perform autoscaling. In InPlace modes it also bounds a single resize cycle; autoscaler resumes on the next poll if a cycle can't finish in time.
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

- **API server 1.33+.** cpvpa checks the server version and the `pods/resize` subresource at startup. If those discovery calls fail, it retries for about 15s and then exits, so the pod restarts and checks again. `InPlace` exits when the cluster lacks support; `InPlaceOrRecreate` falls back to `Recreate` only on a definitive "unsupported" answer, never on a failed call.
- **Kubelet support on the nodes** (`InPlacePodVerticalScaling`, on by default since 1.33). This can differ per node, for example on older node pools within version skew. cpvpa detects it per pod; see *Resize modes* for what happens on nodes without it.
- **`resizePolicy`:** nothing to declare. The default `restartPolicy` is `NotRequired`. Only set `RestartContainer` if the container really needs a restart to pick up the new value; it then restarts on every resize.
- **Limits must allow the requests.** A request may never exceed its limit. If the target sets limits, scale them in the cpvpa config too (see the example), or drop them. cpvpa will not send a resize or template patch that puts a request above its limit.
- **The QoS class cannot change in place.** For a Guaranteed pod (requests equal limits), scale requests and limits together.
- **cpvpa must be the only resizer** for the containers it manages. Exclude them from VPA in-place updates and other autoscalers; otherwise each one reverts the other's change on every poll.

### RBAC

In-place modes need extra permissions beyond the base role. Apply `examples/RBAC/RBAC-inplace-configs.yaml`; it is **self-contained** — it defines the `cpvpa` service account, the base role (nodes, apps) and the in-place add-on role, so use it *instead of* the older `examples/RBAC/RBAC-configs.yaml` (which binds a different service account):

```bash
kubectl apply -f examples/RBAC/RBAC-inplace-configs.yaml
```

This grants:
- `nodes`: `[list, watch]` — the cluster-size signal
- `deployments/replicasets/daemonsets/statefulsets` (`apps`): `[get, patch]` — the workload template (Recreate mode and the fallback)
- `pods`: `[list, get]` — to find and classify pods owned by the target
- `pods/resize`: `[patch]` — the actual in-place resize subresource
- `pods/eviction`: `[create]` — the `InPlaceOrRecreate` fallback's default disruption method (honors PodDisruptionBudgets)

Only when using `--resize-fallback-disruption=delete` instead: add `pods: [delete]` and drop the `pods/eviction` rule (see the commented block in the file).

### Resize modes

- `--resize-mode=Recreate` (default): patch only the template; existing pods are rolled by the controller.
- `--resize-mode=InPlace`: resize live pods via `/resize`. The workload template is left unchanged for self-healing targets (patching it would trigger a full rollout), but non-self-healing targets (bare ReplicaSets, `OnDelete` DaemonSets/StatefulSets) get their template patched too, since that carries no rollout — every pod created afterwards (scale-up, reschedule, eviction) then starts at the right size instead of waiting for the next poll. Pods that report `Deferred` or `Infeasible` are retried on the next poll. Pods whose resize can never succeed are skipped: the node's kubelet lacks in-place support, the change would alter the QoS class or put a request above its limit, or the pod is static, runs on Windows, or uses pod-level resources. Each is logged once as a Warning and counted as `Rejected` in the cycle summary; QoS and limit rejections are re-checked when the target changes. `InPlace` never evicts, deletes or triggers a rollout, so skipped pods keep their old size until something else recreates them. If some nodes lack in-place support, use `InPlaceOrRecreate`. A pod that already has the target size on such a node counts as done.
- `--resize-mode=InPlaceOrRecreate`: like `InPlace`, but pods that fail to resize (kubelet reports `Infeasible` or `Deferred`, the resize stays in progress, or it is skipped as above) for longer than `--resize-fallback-grace-period` are recreated at the new size. The grace clock restarts when cpvpa patches a pod with a new target, except when the kubelet already reported the pod as `Deferred`/`Infeasible` and no value went down: the new size is no easier to fit, and a node count flapping across a step boundary must not postpone the fallback forever. How they're recreated depends on the target: for Deployments, `RollingUpdate` DaemonSets and `RollingUpdate` StatefulSets cpvpa patches the template and lets the controller perform its normal rolling update — which recreates **all** of the workload's pods, paced by `maxUnavailable`/PodDisruptionBudgets, not just the stuck ones; for bare ReplicaSets and `OnDelete` DaemonSets/StatefulSets cpvpa disrupts the stuck pods itself so the controller recreates them. That direct disruption uses the Eviction API by default, which honors PodDisruptionBudgets; with `--resize-fallback-disruption=delete` it is a raw delete that does **not**. Paused Deployments and partitioned StatefulSets are treated as non-self-healing, since their controller would not act on a template patch, so cpvpa disrupts their stuck pods directly. DaemonSet pods are recreated on the same node, so a DaemonSet pod the kubelet reports as `Deferred` or `Infeasible` is never disrupted (its replacement would not fit either); it is logged once as an Error and counted as `FallbackSkippedNodeBound`. DaemonSet pods on nodes without in-place support are still recreated: that is the only way they converge.

Both `InPlace` and `InPlaceOrRecreate` need the in-place RBAC add-on.

In the in-place modes, `--poll-period-seconds` also bounds how long one resize cycle may run: cpvpa tries to resize every matching pod within a single poll period and resumes on the next poll if it can't finish. Already-resized pods are skipped without any API calls, so large workloads converge over a few cycles. For very large workloads, raise the poll period rather than expecting a single cycle to process everything.

### Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--resize-mode` | How to apply resource changes: `Recreate`, `InPlace`, or `InPlaceOrRecreate` | `Recreate` |
| `--resize-fallback-grace-period` | Only for `InPlaceOrRecreate`. How long a pod must fail to resize (Infeasible, Deferred, stuck in progress, or skipped as unresizable) before cpvpa recreates it. | `5m` |
| `--resize-fallback-max-pods-per-cycle` | Only for `InPlaceOrRecreate`. Caps how many stuck pods cpvpa disrupts per cycle (bare ReplicaSet / OnDelete DaemonSet or StatefulSet only; self-healing controllers pace their own rollout). | `1` |
| `--resize-fallback-disruption` | Only for `InPlaceOrRecreate`. How to disrupt stuck pods on non-self-healing targets: `eviction` (uses the Eviction API, honors PodDisruptionBudgets) or `delete` (raw delete, ignores PDBs). | `eviction` |

### Example

See `examples/cpvpa-inplace-example.yaml` for a complete example including:
- a target Deployment whose limits are scaled alongside its requests
- a cpvpa Deployment running in `InPlaceOrRecreate` mode
- the required RBAC bindings
