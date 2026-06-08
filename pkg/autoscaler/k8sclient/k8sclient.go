/*
Copyright 2016 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8sclient

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler/pkg/version"

	"github.com/golang/glog"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sClient - Wraps all needed client functionalities for autoscaler
type K8sClient interface {
	// GetClusterSize counts schedulable nodes and cores in the cluster
	GetClusterSize(ctx context.Context) (*ClusterSize, error)
	// UpdateResources updates the resource needs for the containers in the target.
	// reqsChanged indicates the desired resources differ from what cpvpa last
	// applied; it gates the (disruptive) template patch. When resize mode is
	// InPlace or InPlaceOrRecreate, running pods are converged via the /resize
	// subresource and the template is left untouched on the happy path.
	UpdateResources(ctx context.Context, resources map[string]v1.ResourceRequirements, reqsChanged bool) error
}

// k8sClient - Wraps all Kubernetes API client functionality.
type k8sClient struct {
	target         *targetSpec
	clientset      kubernetes.Interface
	dryRun         bool
	resizeMode     ResizeMode
	fallbackConfig ResizeFallbackConfig
	tracker        *resizeTracker

	cachedSelector labels.Selector
	pollPeriod     time.Duration
}

// NewK8sClient gives a k8sClient with the given dependencies.
func NewK8sClient(namespace, target, kubeconfig string, dryRun bool, mode ResizeMode, fallbackCfg ResizeFallbackConfig, pollPeriod time.Duration) (K8sClient, error) {
	var config *rest.Config
	var err error
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	config.UserAgent = userAgent()
	// Use protobufs for communication with apiserver.
	config.ContentType = "application/vnd.kubernetes.protobuf"
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	tgt, err := makeTarget(clientset, target, namespace)
	if err != nil {
		return nil, err
	}

	if mode == ResizeModeInPlace {
		if err := EnsureResizeSubresource(clientset); err != nil {
			return nil, fmt.Errorf("in-place resize requires the pods/resize subresource: %w", err)
		}
	} else if mode == ResizeModeInPlaceOrRecreate {
		if err := EnsureResizeSubresource(clientset); err != nil {
			glog.Warningf("pods/resize unavailable (%v); %s degrading to %s", err, mode, ResizeModeRecreate)
			mode = ResizeModeRecreate
		}
	}

	var tracker *resizeTracker
	if mode != ResizeModeRecreate {
		tracker = newResizeTracker()
	}

	return newK8sClient(clientset, tgt, dryRun, mode, fallbackCfg, tracker)
}

// newK8sClient builds a k8sClient from its core dependencies.
func newK8sClient(
	clientset kubernetes.Interface,
	target *targetSpec,
	dryRun bool,
	mode ResizeMode,
	fallbackCfg ResizeFallbackConfig,
	tracker *resizeTracker,
) (*k8sClient, error) {
	return &k8sClient{
		clientset:      clientset,
		target:         target,
		dryRun:         dryRun,
		resizeMode:     mode,
		fallbackConfig: fallbackCfg,
		tracker:        tracker,
	}, nil
}

func userAgent() string {
	command := ""
	if len(os.Args) > 0 && len(os.Args[0]) > 0 {
		command = filepath.Base(os.Args[0])
	}
	if len(command) == 0 {
		command = "cpvpa"
	}
	return command + "/" + version.Version
}

func makeTarget(client kubernetes.Interface, target, namespace string) (*targetSpec, error) {
	splits := strings.Split(target, "/")
	if len(splits) != 2 {
		return nil, fmt.Errorf("target format error: %v", target)
	}
	kind := splits[0]
	name := splits[1]

	kind, groupVersions, err := discoverAPI(client, kind)
	if err != nil {
		return nil, err
	}

	tgt, err := newTargetSpec(kind, groupVersions, namespace, name)
	if err != nil {
		return nil, err
	}

	glog.V(4).Infof("Discovered target %s in %v", target, tgt.GroupVersion)
	return tgt, nil
}

func discoverAPI(client kubernetes.Interface, kindArg string) (kind string, groupVersions map[string]bool, err error) {
	var plural string
	switch strings.ToLower(kindArg) {
	case "deployment":
		kind = "Deployment"
		plural = "deployments"
	case "daemonset":
		kind = "DaemonSet"
		plural = "daemonsets"
	case "replicaset":
		kind = "ReplicaSet"
		plural = "replicasets"
	default:
		return "", nil, fmt.Errorf("unknown kind %q", kindArg)
	}

	resourceLists, err := client.Discovery().ServerPreferredNamespacedResources()
	if err != nil {
		if !discovery.IsGroupDiscoveryFailedError(err) {
			return "", nil, fmt.Errorf("failed to discover preferred resources: %v", err)
		}
		glog.Warningf("Failed to discover some groups: %v", err)
	}

	groupVersions = map[string]bool{}
	for _, resourceList := range resourceLists {
		for _, res := range resourceList.APIResources {
			if res.Name == plural && res.Kind == kind {
				groupVersions[resourceList.GroupVersion] = true
			}
		}
	}

	if len(groupVersions) == 0 {
		return "", nil, fmt.Errorf("failed to discover apigroup for kind %q", kind)
	}

	return kind, groupVersions, nil
}

// targetSpec stores the scalable target resource.
type targetSpec struct {
	Kind         string
	GroupVersion string
	Namespace    string
	Name         string
	patcher      patchFunc
}

// Captures the namespace and name to patch, and calls the best
// resource-specific patch method.
type patchFunc func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error

func newTargetSpec(kind string, groupVersions map[string]bool, namespace, name string) (*targetSpec, error) {
	groupVer, patcher, err := findPatcher(kind, groupVersions)
	if err != nil {
		return nil, err
	}

	return &targetSpec{
		Kind:         kind,
		GroupVersion: groupVer,
		Namespace:    namespace,
		Name:         name,
		patcher:      patcher,
	}, nil
}

func (tgt *targetSpec) Patch(ctx context.Context, client kubernetes.Interface, pt types.PatchType, data []byte) error {
	return tgt.patcher(ctx, client, tgt.Namespace, tgt.Name, pt, data)
}

// findPatcher returns a groupVersion string and a patch function for the
// specified kind.  This is needed because, at least in theory, the schema of a
// resource could change dramatically, and we should use statically versioned
// types everywhere.  In practice, it's unlikely that the bits we care about
// would change (since we PATCH).  Alas, there's not a great way to dynamically
// use whatever is "latest".  The fallout of this is that we will need to update
// this program when new API group-versions are introduced.
func findPatcher(kind string, groupVersions map[string]bool) (string, patchFunc, error) {
	switch strings.ToLower(kind) {
	case "deployment":
		return findDeploymentPatcher(groupVersions)
	case "daemonset":
		return findDaemonSetPatcher(groupVersions)
	case "replicaset":
		return findReplicaSetPatcher(groupVersions)
	}
	// This should not happen, we already validated it.
	return "", nil, fmt.Errorf("unknown target kind: %s", kind)
}

func findDeploymentPatcher(groupVersions map[string]bool) (string, patchFunc, error) {
	// Find the best API to use - newest API first.
	if groupVersions["apps/v1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1().Deployments(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta2"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta2().Deployments(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta2", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta1().Deployments(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta1", patchFunc(fn), nil
	}
	if groupVersions["extensions/v1beta1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.ExtensionsV1beta1().Deployments(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "extensions/v1beta1", patchFunc(fn), nil
	}
	return "", nil, fmt.Errorf("no supported API group for target: %v", groupVersions)
}

func findDaemonSetPatcher(groupVersions map[string]bool) (string, patchFunc, error) {
	// Find the best API to use - newest API first.
	if groupVersions["apps/v1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1().DaemonSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta2"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta2().DaemonSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta2", patchFunc(fn), nil
	}
	if groupVersions["extensions/v1beta1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.ExtensionsV1beta1().DaemonSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "extensions/v1beta1", patchFunc(fn), nil
	}
	return "", nil, fmt.Errorf("no supported API group for target: %v", groupVersions)
}

func findReplicaSetPatcher(groupVersions map[string]bool) (string, patchFunc, error) {
	// Find the best API to use - newest API first.
	if groupVersions["apps/v1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1().ReplicaSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta2"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta2().ReplicaSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta2", patchFunc(fn), nil
	}
	if groupVersions["extensions/v1beta1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.ExtensionsV1beta1().ReplicaSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "extensions/v1beta1", patchFunc(fn), nil
	}
	return "", nil, fmt.Errorf("no supported API group for target: %v", groupVersions)
}

// ClusterSize defines the cluster status.
type ClusterSize struct {
	Nodes int
	Cores int
}

func (k *k8sClient) GetClusterSize(ctx context.Context) (clusterStatus *ClusterSize, err error) {
	opt := metav1.ListOptions{Watch: false}

	nodes, err := k.clientset.CoreV1().Nodes().List(ctx, opt)
	if err != nil || nodes == nil {
		return nil, err
	}
	clusterStatus = &ClusterSize{}
	clusterStatus.Nodes = len(nodes.Items)
	var tc resource.Quantity
	// All nodes are considered, even those that are marked as unshedulable,
	// this includes the master.
	for _, node := range nodes.Items {
		tc.Add(node.Status.Capacity[v1.ResourceCPU])
	}

	tcInt64, tcOk := tc.AsInt64()
	if !tcOk {
		return nil, fmt.Errorf("unable to compute integer values of cores in the cluster")
	}
	clusterStatus.Cores = int(tcInt64)
	return clusterStatus, nil
}

func (k *k8sClient) UpdateResources(ctx context.Context, resources map[string]v1.ResourceRequirements, reqsChanged bool) error {
	// Recreate mode: the template is the delivery mechanism. Patch only when
	// the desired values actually changed and let the workload controller
	// perform its normal rolling replacement. Nothing else to do.
	if k.resizeMode == ResizeModeRecreate {
		if !reqsChanged {
			return nil
		}
		return k.patchTemplate(ctx, resources)
	}

	// In-place modes: deliberately do NOT patch the template up front. On a
	// Deployment or RollingUpdate DaemonSet that changes the pod-template-hash
	// and triggers the controller's rolling recreate — destroying the very
	// pods we want to resize live. Existing pods are converged via /resize;
	// pods created later (scale-up, user rollout, reschedule) start at the
	// stale template size and are resized on a subsequent cycle (accepted
	// drift). The template is patched lazily inside the fallback path, and
	// only when a pod must actually be recreated.
	if k.dryRun {
		glog.Infof("dry-run: would in-place resize pods of %s/%s", k.target.Kind, k.target.Name)
		return nil
	}

	// Derive the poll timeout from the configured poll period, with a
	// fallback for test code that constructs k8sClient directly.
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	timeout := k.pollPeriod
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()

	selector, err := k.cachedSelectorOrResolve(ctx)
	if err != nil {
		return fmt.Errorf("resolve selector: %w", err)
	}

	selfHeals := func(ctx context.Context) bool {
		return k.targetSelfHeals(ctx)
	}
	patcher := func(resources map[string]v1.ResourceRequirements) error {
		return k.patchTemplate(ctx, resources)
	}

	result, err := resizeRunningPods(ctx, k.clientset, k.target.Namespace, selector, resources,
		k.resizeMode, k.fallbackConfig, k.tracker, patcher, selfHeals, k.dryRun)
	glog.V(1).Infof("resize cycle: %+v", result)
	if err != nil {
		return fmt.Errorf("in-place resize: %w", err)
	}
	return nil
}

// patchTemplate updates spec.template.spec.containers[].resources on the
// workload. On a Deployment or RollingUpdate DaemonSet this bumps the
// pod-template-hash and triggers the controller's rolling recreate, so it is
// only ever called on the Recreate path and the InPlaceOrRecreate fallback —
// never on a successful in-place resize.
func (k *k8sClient) patchTemplate(ctx context.Context, resources map[string]v1.ResourceRequirements) error {
	ctrs := make([]interface{}, 0, len(resources))
	for ctrName, res := range resources {
		ctrs = append(ctrs, map[string]interface{}{
			"name":      ctrName,
			"resources": res,
		})
	}
	patch := map[string]interface{}{
		"apiVersion": k.target.GroupVersion,
		"kind":       k.target.Kind,
		"metadata": map[string]interface{}{
			"name": k.target.Name,
		},
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"containers": ctrs,
				},
			},
		},
	}

	jb, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("can't marshal template patch to JSON: %v", err)
	}
	if k.dryRun {
		glog.Infof("dry-run: would patch %s/%s template resources", k.target.Kind, k.target.Name)
		return nil
	}
	if err := k.target.Patch(ctx, k.clientset, types.StrategicMergePatchType, jb); err != nil {
		return fmt.Errorf("template patch failed: %v", err)
	}
	return nil
}

// targetSelfHeals reports whether the target controller recreates its pods on
// its own in response to a template change. Deployments and RollingUpdate
// DaemonSets do (the controller paces the replacement via maxUnavailable/PDB,
// so cpvpa must NOT delete pods itself). Bare ReplicaSets/ReplicationControllers
// and OnDelete DaemonSets do not, so cpvpa must delete the pod to force a
// recreate.
func (k *k8sClient) targetSelfHeals(ctx context.Context) bool {
	switch strings.ToLower(k.target.Kind) {
	case "deployment":
		return true
	case "daemonset":
		ds, err := k.clientset.AppsV1().DaemonSets(k.target.Namespace).
			Get(ctx, k.target.Name, metav1.GetOptions{})
		if err != nil {
			// Unknown strategy: assume NOT self-healing so the fallback deletes
			// the stuck pod directly. Assuming self-healing here would, for an
			// OnDelete DaemonSet, patch the template and then wait forever for a
			// rollout the controller never performs, leaving the pod stuck.
			glog.Errorf("self-heal check: get daemonset %s/%s: %v; assuming non-self-healing (will delete pods directly)",
				k.target.Namespace, k.target.Name, err)
			return false
		}
		return ds.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType
	default: // bare ReplicaSet — not owned by a higher controller, so autoscaler deletes pods itself
		return false
	}
}

// EnsureResizeSubresource checks that the cluster supports the pods/resize
// subresource (Kubernetes 1.33+). Call at startup when resize mode is not
// Recreate to fail fast with a clear message.
//
// Note: this checks discovery (the API server advertises the subresource),
// but it cannot confirm the InPlacePodVerticalScaling feature gate is
// enabled on every kubelet. As of Kubernetes 1.33 the gate is on by
// default, so discovery presence is a sufficient signal.
func EnsureResizeSubresource(client kubernetes.Interface) error {
	resources, err := client.Discovery().ServerResourcesForGroupVersion("v1")
	if err != nil {
		return fmt.Errorf("failed to discover v1 API resources: %w", err)
	}
	for _, r := range resources.APIResources {
		if r.Name == "pods/resize" {
			return nil
		}
	}
	return fmt.Errorf("cluster does not support pods/resize subresource; " +
		"in-place pod resize requires Kubernetes 1.33+ with InPlacePodVerticalScaling enabled")
}

func (k *k8sClient) getTargetSelector(ctx context.Context) (string, error) {
	switch strings.ToLower(k.target.Kind) {
	case "deployment":
		dep, err := k.clientset.AppsV1().Deployments(k.target.Namespace).Get(ctx, k.target.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(dep.Spec.Selector), nil
	case "daemonset":
		ds, err := k.clientset.AppsV1().DaemonSets(k.target.Namespace).Get(ctx, k.target.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(ds.Spec.Selector), nil
	case "replicaset":
		rs, err := k.clientset.AppsV1().ReplicaSets(k.target.Namespace).Get(ctx, k.target.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(rs.Spec.Selector), nil
	}
	return "", fmt.Errorf("unknown target kind: %s", k.target.Kind)
}

// cachedSelectorOrResolve returns the cached selector if available,
// otherwise fetches it from the target workload, parses it, and caches
// it for subsequent cycles. Selectors are immutable for Deployment,
// DaemonSet, and ReplicaSet, so the cache never needs invalidation.
func (k *k8sClient) cachedSelectorOrResolve(ctx context.Context) (labels.Selector, error) {
	if k.cachedSelector != nil {
		return k.cachedSelector, nil
	}
	selectorStr, err := k.getTargetSelector(ctx)
	if err != nil {
		return nil, err
	}
	selector, err := labels.Parse(selectorStr)
	if err != nil {
		return nil, err
	}
	k.cachedSelector = selector
	return selector, nil
}
