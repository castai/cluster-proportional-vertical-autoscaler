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
	"k8s.io/utils/clock"
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
	target     *targetMeta
	clientset  kubernetes.Interface
	dryRun     bool
	resizeMode ResizeMode
	podResizer *podResizer
}

// NewK8sClient gives a k8sClient with the given dependencies.
func NewK8sClient(namespace, target, kubeconfig string, dryRun bool, mode ResizeMode, fallbackCfg ResizeFallbackConfig, clk clock.PassiveClock) (K8sClient, error) {
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

	switch mode {
	case ResizeModeInPlace:
		if err := EnsureResizeSubresource(clientset); err != nil {
			return nil, fmt.Errorf("in-place resize requires the pods/resize subresource: %w", err)
		}
	case ResizeModeInPlaceOrRecreate:
		if err := EnsureResizeSubresource(clientset); err != nil {
			glog.Warningf("pods/resize unavailable (%v); %s degrading to %s", err, mode, ResizeModeRecreate)
			mode = ResizeModeRecreate
		}
	}

	var resizer *podResizer
	if mode != ResizeModeRecreate {
		resizer = &podResizer{
			resizeMode:     mode,
			fallbackConfig: fallbackCfg,
			dryRun:         dryRun,
			clock:          clk,
			clientset:      clientset,
			tracker:        newResizeTracker(),
		}
	}

	return newK8sClient(clientset, tgt, resizer, mode)
}

// newK8sClient builds a k8sClient from its core dependencies.
func newK8sClient(
	clientset kubernetes.Interface,
	target *targetMeta,
	podResizer *podResizer,
	mode ResizeMode,
) (*k8sClient, error) {
	return &k8sClient{
		clientset:  clientset,
		target:     target,
		podResizer: podResizer,
		resizeMode: mode,
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

func makeTarget(client kubernetes.Interface, target, namespace string) (*targetMeta, error) {
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

	tgt, err := newTargetMeta(kind, groupVersions, namespace, name)
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
	case "statefulset":
		kind = "StatefulSet"
		plural = "statefulsets"
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

// targetMeta stores the scalable target resource.
type targetMeta struct {
	Kind         string
	GroupVersion string
	Namespace    string
	Name         string
	patcher      patchFunc
}

// Captures the namespace and name to patch, and calls the best
// resource-specific patch method.
type patchFunc func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error

func newTargetMeta(kind string, groupVersions map[string]bool, namespace, name string) (*targetMeta, error) {
	groupVer, patcher, err := findPatcher(kind, groupVersions)
	if err != nil {
		return nil, err
	}

	return &targetMeta{
		Kind:         kind,
		GroupVersion: groupVer,
		Namespace:    namespace,
		Name:         name,
		patcher:      patcher,
	}, nil
}

func (tgt *targetMeta) Patch(ctx context.Context, client kubernetes.Interface, pt types.PatchType, data []byte) error {
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
	case "statefulset":
		return findStatefulSetPatcher(groupVersions)
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

func findStatefulSetPatcher(groupVersions map[string]bool) (string, patchFunc, error) {
	// Find the best API to use - newest API first.
	if groupVersions["apps/v1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1().StatefulSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta2"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta2().StatefulSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta2", patchFunc(fn), nil
	}
	if groupVersions["apps/v1beta1"] {
		fn := func(ctx context.Context, client kubernetes.Interface, namespace, name string, pt types.PatchType, data []byte) error {
			_, err := client.AppsV1beta1().StatefulSets(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
			return err
		}
		return "apps/v1beta1", patchFunc(fn), nil
	}
	return "", nil, fmt.Errorf("no supported API group for target: %v", groupVersions)
}

type targetSpec struct {
	UID           types.UID
	PodSelector   labels.Selector
	IsSelfHealing bool
	// TemplateResources is the current per-container resources in the
	// workload's pod template, captured when the spec is fetched. It lets
	// PatchTemplate detect when a template patch would be a no-op.
	TemplateResources map[string]v1.ResourceRequirements
}

// targetClient encapsulates the target workload object and its client
// dependencies for querying and patching.
type targetClient struct {
	meta      targetMeta
	clientset kubernetes.Interface
	dryRun    bool

	cachedSpec *targetSpec
}

// PatchTemplate updates spec.template.spec.containers[].resources on the workload.
// It returns whether the patch actually changed the template: if the template
// is already known to match desired for every managed container, no patch is
// issued and changed is false.
//
// This method deliberately does NOT fetch the spec here as it resolves the target
// via apps/v1, which is correct for the in-place modes (they require Kubernetes
// 1.33+, where these kinds exist only in apps/v1) but not necessarily for
// Recreate mode, which must keep working on older clusters whose target lives
// in an older API group.
func (t *targetClient) PatchTemplate(ctx context.Context, resources map[string]v1.ResourceRequirements) (changed bool, err error) {
	if t.cachedSpec != nil && templateMatches(t.cachedSpec.TemplateResources, resources) {
		return false, nil // already at desired; patching would be a no-op
	}
	if t.dryRun {
		glog.Infof("dry-run: would patch %s/%s template resources", t.meta.Kind, t.meta.Name)
		return true, nil
	}
	ctrs := make([]interface{}, 0, len(resources))
	for ctrName, res := range resources {
		ctrs = append(ctrs, map[string]interface{}{
			"name":      ctrName,
			"resources": res,
		})
	}
	patch := map[string]interface{}{
		"apiVersion": t.meta.GroupVersion,
		"kind":       t.meta.Kind,
		"metadata": map[string]interface{}{
			"name": t.meta.Name,
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
		return false, fmt.Errorf("can't marshal template patch to JSON: %v", err)
	}
	if err := t.meta.Patch(ctx, t.clientset, types.StrategicMergePatchType, jb); err != nil {
		return false, fmt.Errorf("template patch failed: %v", err)
	}
	return true, nil
}

// containerResources extracts a name->resources map from a container list.
func containerResources(containers []v1.Container) map[string]v1.ResourceRequirements {
	out := make(map[string]v1.ResourceRequirements, len(containers))
	for i := range containers {
		out[containers[i].Name] = containers[i].Resources
	}
	return out
}

// templateMatches reports whether have already satisfies desired for every
// managed container (a missing container counts as a mismatch).
func templateMatches(have, desired map[string]v1.ResourceRequirements) bool {
	for name, want := range desired {
		got, ok := have[name]
		if !ok || !resourcesSatisfied(got, want) {
			return false
		}
	}
	return true
}

// GetOwnedPods returns the live pods owned by this target.  It fetches the
// selector, lists pods in the target namespace, and filters out pods not
// owned by the target.
func (t *targetClient) GetOwnedPods(ctx context.Context) ([]v1.Pod, error) {
	spec, err := t.trySyncSpec(ctx)
	if err != nil {
		return nil, err
	}

	list, err := t.clientset.CoreV1().Pods(t.meta.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: spec.PodSelector.String(),
	})
	if err != nil {
		return nil, err
	}

	var owned []v1.Pod
	for i := range list.Items {
		pod := &list.Items[i]
		if !t.ownsPod(*spec, pod) {
			continue
		}
		owned = append(owned, *pod)
	}
	return owned, nil
}

// ownsPod reports whether pod is controlled by this target.
//
// For ReplicaSet and DaemonSet targets the pod's controller is the target
// itself, so we compare UIDs directly.
//
// For Deployment targets the pod is owned by a ReplicaSet, which is in turn
// owned by the Deployment. We rely on the Deployment controller's RS naming
// convention "<deployment-name>-<pod-template-hash>".
func (t *targetClient) ownsPod(spec targetSpec, pod *v1.Pod) bool {
	ctrl := metav1.GetControllerOf(pod)
	if ctrl == nil {
		return false // orphan or no controlling owner
	}
	ctrlKind := strings.ToLower(ctrl.Kind)
	switch strings.ToLower(t.meta.Kind) {
	case "replicaset":
		return ctrlKind == "replicaset" && spec.UID != "" && ctrl.UID == spec.UID
	case "daemonset":
		return ctrlKind == "daemonset" && spec.UID != "" && ctrl.UID == spec.UID
	case "statefulset":
		return ctrlKind == "statefulset" && spec.UID != "" && ctrl.UID == spec.UID
	case "deployment":
		if ctrlKind != "replicaset" {
			return false
		}
		prefix := t.meta.Name + "-"
		if !strings.HasPrefix(ctrl.Name, prefix) {
			return false
		}
		hash := ctrl.Name[len(prefix):]
		return hash != "" && !strings.Contains(hash, "-")
	default:
		return false
	}
}

func (t *targetClient) trySyncSpec(ctx context.Context) (*targetSpec, error) {
	if t.cachedSpec != nil {
		return t.cachedSpec, nil
	}

	spec, err := t.fetchSpec(ctx)
	if err != nil {
		return nil, err
	}

	t.cachedSpec = spec
	return spec, nil
}

func (t *targetClient) fetchSpec(ctx context.Context) (*targetSpec, error) {
	var spec targetSpec
	var selector *metav1.LabelSelector

	switch strings.ToLower(t.meta.Kind) {
	case "deployment":
		dep, err := t.clientset.AppsV1().Deployments(t.meta.Namespace).Get(ctx, t.meta.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		selector = dep.Spec.Selector
		spec.UID = dep.UID
		spec.IsSelfHealing = true
		spec.TemplateResources = containerResources(dep.Spec.Template.Spec.Containers)
	case "daemonset":
		ds, err := t.clientset.AppsV1().DaemonSets(t.meta.Namespace).Get(ctx, t.meta.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		selector = ds.Spec.Selector
		spec.UID = ds.UID
		spec.IsSelfHealing = ds.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType
		spec.TemplateResources = containerResources(ds.Spec.Template.Spec.Containers)
	case "replicaset":
		rs, err := t.clientset.AppsV1().ReplicaSets(t.meta.Namespace).Get(ctx, t.meta.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		selector = rs.Spec.Selector
		spec.UID = rs.UID
		spec.TemplateResources = containerResources(rs.Spec.Template.Spec.Containers)
	case "statefulset":
		ss, err := t.clientset.AppsV1().StatefulSets(t.meta.Namespace).Get(ctx, t.meta.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		selector = ss.Spec.Selector
		spec.UID = ss.UID
		spec.IsSelfHealing = ss.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType
		spec.TemplateResources = containerResources(ss.Spec.Template.Spec.Containers)
	default:
		return nil, fmt.Errorf("unknown target kind: %s", t.meta.Kind)
	}

	if selector, err := metav1.LabelSelectorAsSelector(selector); err != nil {
		return nil, err
	} else {
		spec.PodSelector = selector
	}

	return &spec, nil
}

// IsSelfHealing reports whether the target controller recreates its pods on
// its own in response to a template change.
func (t *targetClient) IsSelfHealing(ctx context.Context) (bool, error) {
	spec, err := t.trySyncSpec(ctx)
	if err != nil {
		return false, err
	}
	return spec.IsSelfHealing, nil
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
	target := &targetClient{
		meta:      *k.target,
		clientset: k.clientset,
		dryRun:    k.dryRun,
	}

	if k.resizeMode == ResizeModeRecreate {
		if !reqsChanged {
			return nil
		}
		_, err := target.PatchTemplate(ctx, resources)
		return err
	}

	result, err := k.podResizer.resizeRunningPods(ctx, target, resources)
	glog.V(1).Infof("resize cycle: %+v", result)
	if err != nil {
		return fmt.Errorf("in-place resize: %w", err)
	}
	return nil
}
