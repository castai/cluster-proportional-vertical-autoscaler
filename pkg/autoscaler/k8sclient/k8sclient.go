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
	"reflect"
	"strings"
	"time"

	"github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler/pkg/version"

	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
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
	GetClusterSize() (*ClusterSize, error)
	// UpdateResources updates the resource needs for the containers in the target.
	// When resize mode is InPlace or InPlaceOrRecreate, it also patches running
	// pods via the /resize subresource.
	UpdateResources(resources map[string]apiv1.ResourceRequirements) error
}

// k8sClient - Wraps all Kubernetes API client functionality.
type k8sClient struct {
	target         *targetSpec
	clientset      kubernetes.Interface
	clusterStatus  *ClusterSize
	dryRun         bool
	resizeMode     ResizeMode
	fallbackConfig FallbackConfig
	tracker        *resizeTracker
	lastResources  map[string]apiv1.ResourceRequirements
	cachedSelector labels.Selector
	ctx            context.Context
	pollPeriod     time.Duration
	metrics        *Metrics
}

// NewK8sClient gives a k8sClient with the given dependencies.
func NewK8sClient(namespace, target, kubeconfig string, dryRun bool, mode ResizeMode, fallbackCfg FallbackConfig, pollPeriod time.Duration, stopCh <-chan struct{}) (K8sClient, error) {
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

	if mode != ResizeModeRecreate {
		if err := EnsureResizeSubresource(clientset); err != nil {
			return nil, err
		}
	}

	var tracker *resizeTracker
	if mode != ResizeModeRecreate {
		tracker = newResizeTracker()
	}

	ctx := context.Background()
	if stopCh != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		go func() {
			<-stopCh
			cancel()
		}()
	}

	return &k8sClient{
		clientset:      clientset,
		target:         tgt,
		dryRun:         dryRun,
		resizeMode:     mode,
		fallbackConfig: fallbackCfg,
		tracker:        tracker,
		ctx:            ctx,
		pollPeriod:     pollPeriod,
		metrics:        &Metrics{},
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
type patchFunc func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error

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

func (tgt *targetSpec) Patch(client kubernetes.Interface, pt types.PatchType, data []byte) error {
	return tgt.patcher(client, tgt.Namespace, tgt.Name, pt, data)
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
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1().Deployments(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta2"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta2().Deployments(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta2", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta1"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta1().Deployments(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta1", patchFunc(fn), nil
	}
	if groupVersions["extensions/v1beta1"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.ExtensionsV1beta1().Deployments(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "extensions/v1beta1", patchFunc(fn), nil
	}
	return "", nil, fmt.Errorf("no supported API group for target: %v", groupVersions)
}

func findDaemonSetPatcher(groupVersions map[string]bool) (string, patchFunc, error) {
	// Find the best API to use - newest API first.
	if groupVersions["apps/v1"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1().DaemonSets(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta2"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta2().DaemonSets(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta2", patchFunc(fn), nil
	}
	if groupVersions["extensions/v1beta1"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.ExtensionsV1beta1().DaemonSets(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "extensions/v1beta1", patchFunc(fn), nil
	}
	return "", nil, fmt.Errorf("no supported API group for target: %v", groupVersions)
}

func findReplicaSetPatcher(groupVersions map[string]bool) (string, patchFunc, error) {
	// Find the best API to use - newest API first.
	if groupVersions["apps/v1"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1().ReplicaSets(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta2"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta2().ReplicaSets(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta2", patchFunc(fn), nil
	}
	if groupVersions["extensions/v1beta1"] {
		fn := func(client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.ExtensionsV1beta1().ReplicaSets(namespace).Patch(context.TODO(), name, pt, data, metav1.PatchOptions{})
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

func (k *k8sClient) GetClusterSize() (clusterStatus *ClusterSize, err error) {
	opt := metav1.ListOptions{Watch: false}

	nodes, err := k.clientset.CoreV1().Nodes().List(context.TODO(), opt)
	if err != nil || nodes == nil {
		return nil, err
	}
	clusterStatus = &ClusterSize{}
	clusterStatus.Nodes = len(nodes.Items)
	var tc resource.Quantity
	// All nodes are considered, even those that are marked as unshedulable,
	// this includes the master.
	for _, node := range nodes.Items {
		tc.Add(node.Status.Capacity[apiv1.ResourceCPU])
	}

	tcInt64, tcOk := tc.AsInt64()
	if !tcOk {
		return nil, fmt.Errorf("unable to compute integer values of cores in the cluster")
	}
	clusterStatus.Cores = int(tcInt64)
	k.clusterStatus = clusterStatus
	return clusterStatus, nil
}

func (k *k8sClient) UpdateResources(resources map[string]apiv1.ResourceRequirements) error {
	// Skip the template patch when nothing changed. The pod convergence
	// loop still runs because running pods may need to be caught up even
	// when the template is already correct.
	templateChanged := !reflect.DeepEqual(k.lastResources, resources)

	if templateChanged {
		ctrs := []interface{}{}
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
			return fmt.Errorf("can't marshal patch to JSON: %v", err)
		}

		if k.dryRun {
			glog.V(1).Infof("dry-run: would patch template for %s/%s", k.target.Kind, k.target.Name)
		} else {
			if err := k.target.Patch(k.clientset, types.StrategicMergePatchType, jb); err != nil {
				return fmt.Errorf("patch failed: %v", err)
			}
		}
		k.lastResources = resources
	}

	if k.resizeMode == ResizeModeRecreate {
		return nil
	}

	// Derive the poll timeout from the configured poll period, with a
	// fallback for test code that constructs k8sClient directly.
	baseCtx := k.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	timeout := k.pollPeriod
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()

	selector, err := k.cachedSelectorOrResolve()
	if err != nil {
		glog.Errorf("in-place resize: failed to resolve selector: %v", err)
		return nil
	}

	result, err := k.resizeRunningPods(ctx, k.target.Namespace, selector, resources, k.resizeMode, k.fallbackConfig, k.tracker)
	if err != nil {
		glog.Errorf("in-place resize failed (template already updated): %v", err)
	}
	k.metrics.Record(result)
	m := k.metrics.Snapshot()
	glog.V(1).Infof("resize cycle: %+v (cumulative: Applied=%d Deferred=%d Infeasible=%d Evicted=%d Errors=%d)",
		result, m.Applied, m.Deferred, m.Infeasible, m.Evicted, m.Errors)
	return nil
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

func (k *k8sClient) getTargetSelector() (string, error) {
	switch strings.ToLower(k.target.Kind) {
	case "deployment":
		dep, err := k.clientset.AppsV1().Deployments(k.target.Namespace).Get(context.TODO(), k.target.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(dep.Spec.Selector), nil
	case "daemonset":
		ds, err := k.clientset.AppsV1().DaemonSets(k.target.Namespace).Get(context.TODO(), k.target.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(ds.Spec.Selector), nil
	case "replicaset":
		rs, err := k.clientset.AppsV1().ReplicaSets(k.target.Namespace).Get(context.TODO(), k.target.Name, metav1.GetOptions{})
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
func (k *k8sClient) cachedSelectorOrResolve() (labels.Selector, error) {
	if k.cachedSelector != nil {
		return k.cachedSelector, nil
	}
	selectorStr, err := k.getTargetSelector()
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
