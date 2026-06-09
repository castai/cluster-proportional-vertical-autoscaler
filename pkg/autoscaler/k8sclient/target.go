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
	"strings"

	"github.com/golang/glog"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

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

// targetClient encapsulates the target workload object and its client
// dependencies for querying and patching.
type targetClient struct {
	spec targetSpec

	clientset kubernetes.Interface
	patcher   patchFunc
	dryRun    bool

	cachedSelector      labels.Selector
	cachedIsSelfHealing *bool
}

// newTargetClient builds a targetClient from a targetSpec and its dependencies.
func newTargetClient(spec targetSpec, clientset kubernetes.Interface, dryRun bool) *targetClient {
	return &targetClient{
		spec:     spec,
		clientset: clientset,
		patcher:  spec.patcher,
		dryRun:   dryRun,
	}
}

// Namespace returns the namespace of the target workload.
func (t *targetClient) Namespace() string {
	return t.spec.Namespace
}

// GetPodSelector returns the label selector for the target workload. The
// result is cached because selectors are immutable for Deployment, DaemonSet,
// and ReplicaSet, so the cache never needs invalidation within a process
// lifetime.
func (t *targetClient) GetPodSelector(ctx context.Context) (labels.Selector, error) {
	if t.cachedSelector != nil {
		return t.cachedSelector, nil
	}
	selectorStr, err := t.getTargetSelector(ctx)
	if err != nil {
		return nil, err
	}
	selector, err := labels.Parse(selectorStr)
	if err != nil {
		return nil, err
	}
	t.cachedSelector = selector
	return selector, nil
}

// getTargetSelector returns the raw selector string for the target workload.
func (t *targetClient) getTargetSelector(ctx context.Context) (string, error) {
	switch strings.ToLower(t.spec.Kind) {
	case "deployment":
		dep, err := t.clientset.AppsV1().Deployments(t.spec.Namespace).Get(ctx, t.spec.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(dep.Spec.Selector), nil
	case "daemonset":
		ds, err := t.clientset.AppsV1().DaemonSets(t.spec.Namespace).Get(ctx, t.spec.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(ds.Spec.Selector), nil
	case "replicaset":
		rs, err := t.clientset.AppsV1().ReplicaSets(t.spec.Namespace).Get(ctx, t.spec.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return metav1.FormatLabelSelector(rs.Spec.Selector), nil
	}
	return "", fmt.Errorf("unknown target kind: %s", t.spec.Kind)
}

// IsSelfHealing reports whether the target controller recreates its pods on
// its own in response to a template change. Deployments and RollingUpdate
// DaemonSets do (the controller paces the replacement via maxUnavailable/PDB,
// so cpvpa must NOT delete pods itself). Bare ReplicaSets/ReplicationControllers
// and OnDelete DaemonSets do not, so cpvpa must delete the pod to force a
// recreate.
//
// On API error it falls back internally: if a cached value exists it is
// reused; otherwise it returns false (safe default: direct delete).
func (t *targetClient) IsSelfHealing(ctx context.Context) bool {
	switch strings.ToLower(t.spec.Kind) {
	case "deployment":
		return true
	case "daemonset":
		ds, err := t.clientset.AppsV1().DaemonSets(t.spec.Namespace).
			Get(ctx, t.spec.Name, metav1.GetOptions{})
		if err != nil {
			if t.cachedIsSelfHealing != nil {
				glog.V(2).Infof("self-heal check: get daemonset %s/%s: %v; using cached value (%t)",
					t.spec.Namespace, t.spec.Name, err, *t.cachedIsSelfHealing)
				return *t.cachedIsSelfHealing
			}
			glog.Errorf("self-heal check: get daemonset %s/%s: %v; assuming non-self-healing (will delete pods directly)",
				t.spec.Namespace, t.spec.Name, err)
			return false
		}
		selfHeals := ds.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType
		t.cachedIsSelfHealing = &selfHeals
		return selfHeals
	default: // bare ReplicaSet — not owned by a higher controller, so autoscaler deletes pods itself
		return false
	}
}

// PatchTemplate updates spec.template.spec.containers[].resources on the
// workload. On a Deployment or RollingUpdate DaemonSet this bumps the
// pod-template-hash and triggers the controller's rolling recreate, so it is
// only ever called on the Recreate path and the InPlaceOrRecreate fallback —
// never on a successful in-place resize.
func (t *targetClient) PatchTemplate(ctx context.Context, resources map[string]v1.ResourceRequirements) error {
	if t.dryRun {
		glog.Infof("dry-run: would patch %s/%s template resources", t.spec.Kind, t.spec.Name)
		return nil
	}
	ctrs := make([]interface{}, 0, len(resources))
	for ctrName, res := range resources {
		ctrs = append(ctrs, map[string]interface{}{
			"name":      ctrName,
			"resources": res,
		})
	}
	patch := map[string]interface{}{
		"apiVersion": t.spec.GroupVersion,
		"kind":       t.spec.Kind,
		"metadata": map[string]interface{}{
			"name": t.spec.Name,
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
	if err := t.spec.Patch(ctx, t.clientset, types.StrategicMergePatchType, jb); err != nil {
		return fmt.Errorf("template patch failed: %v", err)
	}
	return nil
}