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
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

// TestRBACInplaceCoversAllTargetKinds is a guard test for the shipped RBAC
// example: every workload kind that discoverAPI accepts must be readable and
// patchable by the cpvpa ClusterRole in RBAC-inplace-configs.yaml, otherwise
// targets of that kind fail every poll with 403.
func TestRBACInplaceCoversAllTargetKinds(t *testing.T) {
	data, err := os.ReadFile("../../../examples/RBAC/RBAC-inplace-configs.yaml")
	if err != nil {
		t.Fatalf("read RBAC example: %v", err)
	}

	type rbacRule struct {
		APIGroups []string `json:"apiGroups"`
		Resources []string `json:"resources"`
		Verbs     []string `json:"verbs"`
	}
	type rbacDoc struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Rules []rbacRule `json:"rules"`
	}

	var appsRule *rbacRule
	var nodeVerbs []string
	for _, doc := range strings.Split(string(data), "\n---") {
		var d rbacDoc
		if err := yaml.Unmarshal([]byte(doc), &d); err != nil {
			continue // comment-only or empty segment
		}
		if d.Kind == "ClusterRole" && d.Metadata.Name == "cpvpa" {
			for i := range d.Rules {
				for _, g := range d.Rules[i].APIGroups {
					if g == "apps" {
						r := d.Rules[i]
						appsRule = &r
					}
					if g == "" {
						for _, res := range d.Rules[i].Resources {
							if res == "nodes" {
								nodeVerbs = append(nodeVerbs, d.Rules[i].Verbs...)
							}
						}
					}
				}
			}
		}
	}
	if appsRule == nil {
		t.Fatal("no apps rule found in the cpvpa ClusterRole")
	}

	has := func(list []string, want string) bool {
		for _, s := range list {
			if s == want {
				return true
			}
		}
		return false
	}
	for _, kind := range []string{"deployments", "daemonsets", "replicasets", "statefulsets"} {
		if !has(appsRule.Resources, kind) {
			t.Errorf("cpvpa ClusterRole apps rule is missing %q (targets of this kind get 403)", kind)
		}
	}
	for _, verb := range []string{"get", "patch"} {
		if !has(appsRule.Verbs, verb) {
			t.Errorf("cpvpa ClusterRole apps rule is missing verb %q", verb)
		}
	}
	// list: cluster size every poll; get: node allocatable in the node-bound
	// fallback log (a 403 there is swallowed, so nothing else would notice).
	for _, verb := range []string{"get", "list"} {
		if !has(nodeVerbs, verb) {
			t.Errorf("cpvpa ClusterRole is missing verb %q on nodes", verb)
		}
	}
}

func TestDiscoverAPI(t *testing.T) {
	testCases := []struct {
		kind     string
		expError bool
	}{
		{
			"deployment",
			false,
		},
		{
			"daemonset",
			false,
		},
		{
			"replicaset",
			false,
		},
		{
			"statefulset",
			false,
		},
		{
			"replicationcontroller",
			true,
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var obj interface{}
		stable := metav1.APIResourceList{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment"},
				{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
				{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				{Name: "statefulsets", Namespaced: true, Kind: "StatefulSet"},
			},
		}

		groups := metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "custom.group.example.com",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "custom.group.example.com/v1alpha1", Version: "v1alpha1"},
					},
					PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "custom.group.example.com/v1alpha1", Version: "v1alpha1"},
				},
			},
		}

		switch req.URL.Path {
		case "/api":
			obj = &metav1.APIVersions{
				Versions: []string{
					"v1",
				},
			}
		case "/api/v1":
			obj = &stable
		case "/apis":
			obj = &groups
		case "/apis/custom.group.example.com/v1alpha1":
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}

		output, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("unexpected encoding error: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(output)
	}))
	defer server.Close()

	for _, tc := range testCases {
		_, _, err := discoverAPI(
			clientset.NewForConfigOrDie(&restclient.Config{
				Host: server.URL,
				ContentConfig: restclient.ContentConfig{
					GroupVersion: &schema.GroupVersion{Group: tc.kind, Version: "v1"}}}),
			tc.kind)

		if err != nil && !tc.expError {
			t.Errorf("Expect no error, got error for kind: %q: %v", tc.kind, err)
			continue
		} else if err == nil && tc.expError {
			t.Errorf("Expect error, got no error for kind: %q", tc.kind)
			continue
		}
	}
}

func TestUpdateResources(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var obj interface{}
		groups := metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "extensions",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "extensions/v1beta1", Version: "v1beta1"},
					},
					PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "extensions/v1beta1", Version: "v1beta1"},
				},
				{
					Name: "apps",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "apps/v1", Version: "v1"},
					},
					PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"},
				},
			},
		}
		extensionsResources := metav1.APIResourceList{
			GroupVersion: "extensions/v1beta1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment"},
				{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
				{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
			},
		}
		appsResources := metav1.APIResourceList{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment"},
				{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
				{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				{Name: "statefulsets", Namespaced: true, Kind: "StatefulSet"},
			},
		}
		switch req.URL.Path {
		case "/api":
			obj = &metav1.APIVersions{
				Versions: []string{
					"extensions/v1beta1",
				},
			}
		case "/apis":
			obj = &groups
		case "/apis/extensions/v1beta1":
			obj = &extensionsResources
		case "/apis/apps/v1":
			obj = &appsResources
		case "/apis/extensions/v1beta1/namespaces/default/daemonsets/thing":
		case "/apis/extensions/v1beta1/namespaces/default/replicasets/thing":
		case "/apis/extensions/v1beta1/namespaces/default/deployments/thing":
		case "/apis/apps/v1/namespaces/default/deployments/thing":
		case "/apis/apps/v1/namespaces/default/daemonsets/thing":
		case "/apis/apps/v1/namespaces/default/replicasets/thing":
		case "/apis/apps/v1/namespaces/default/statefulsets/thing":
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		output, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("unexpected encoding error: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(output)
	}))
	defer server.Close()

	testCases := []struct {
		target   string
		kind     string
		res      int
		expError bool
	}{
		{
			"deployment/thing",
			"deployment",
			10,
			false,
		},
		{

			"daemonset/thing",
			"daemonSet",
			20,
			false,
		},
		{
			"replicaset/thing",
			"replicaSet",
			30,
			false,
		},
		{
			"statefulset/thing",
			"statefulSet",
			40,
			false,
		},
	}

	for _, tc := range testCases {
		client := clientset.NewForConfigOrDie(&restclient.Config{
			Host: server.URL,
			ContentConfig: restclient.ContentConfig{
				GroupVersion: &schema.GroupVersion{Group: tc.kind, Version: "extensions/v1beta1"}}})

		tgt, err := makeTarget(client, tc.target, "default")
		if err != nil {
			t.Fatalf("error making target %q: %v", tc.target, err)
		}
		k8scli, err := newK8sClient(client, tgt, nil, ResizeModeRecreate, false)
		if err != nil {
			t.Fatalf("error creating k8sClient: %v", err)
		}

		newReqs := map[string]apiv1.ResourceRequirements{}
		newReqs["thing"] = apiv1.ResourceRequirements{
			Requests: map[apiv1.ResourceName]resource.Quantity{},
			Limits:   map[apiv1.ResourceName]resource.Quantity{},
		}
		r := resource.NewQuantity(0, resource.BinarySI)
		r.SetMilli(10)
		newReqs["thing"].Requests[apiv1.ResourceName("cpu")] = *r
		if err := k8scli.UpdateResources(context.Background(), newReqs, true); err != nil {
			t.Errorf("failed to update resources for target %q: %v", tc.target, err)
		}
	}
}

// TestLogCycle verifies the cycle summary is raised to Warning only when the
// problem counts change to a non-zero value, not on every poll while a
// problem persists, and that a node-bound skip counts as a problem.
func TestLogCycle(t *testing.T) {
	warnings := 0
	orig := logWarningf
	logWarningf = func(string, ...interface{}) { warnings++ }
	defer func() { logWarningf = orig }()

	k := &k8sClient{}
	for i, step := range []struct {
		result   resizeResult
		wantWarn int // cumulative
	}{
		{resizeResult{TargetPods: 3, AlreadyOK: 3}, 0},
		{resizeResult{TargetPods: 3, Rejected: 1, Unsupported: 1}, 1},
		{resizeResult{TargetPods: 3, Rejected: 1, Unsupported: 1, Applied: 2}, 1}, // same problems
		{resizeResult{TargetPods: 3, Rejected: 1, Unsupported: 1}, 1},
		{resizeResult{TargetPods: 3, FallbackSkippedNodeBound: 1}, 2},
		{resizeResult{TargetPods: 3, AlreadyOK: 3}, 2}, // cleared: Info, not Warning
		{resizeResult{TargetPods: 3, Errors: 1}, 3},
	} {
		k.logCycle(step.result)
		if warnings != step.wantWarn {
			t.Errorf("step %d: warnings = %d, want %d", i, warnings, step.wantWarn)
		}
	}
}

// TestPatchTemplate_CacheFollowsPatch: after a successful template patch the
// cached template reflects it, so a second PatchTemplate with the same values
// in the same cycle (the fallback after UpdateResources) sends nothing.
func TestPatchTemplate_CacheFollowsPatch(t *testing.T) {
	rs := &appsv1.ReplicaSet{
		TypeMeta:   metav1.TypeMeta{Kind: "ReplicaSet", APIVersion: "apps/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "rs-uid"},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: apiv1.PodTemplateSpec{Spec: apiv1.PodSpec{Containers: []apiv1.Container{{
				Name:      "main",
				Resources: apiv1.ResourceRequirements{Requests: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("100m")}},
			}}}},
		},
	}
	patches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var obj interface{}
		switch {
		case req.URL.Path == "/api":
			obj = &metav1.APIVersions{Versions: []string{"v1"}}
		case req.URL.Path == "/apis":
			obj = &metav1.APIGroupList{Groups: []metav1.APIGroup{{
				Name:             "apps",
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: "apps/v1", Version: "v1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"},
			}}}
		case req.URL.Path == "/apis/apps/v1":
			obj = &metav1.APIResourceList{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{
				{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
			}}
		case req.URL.Path == "/apis/apps/v1/namespaces/default/replicasets/web":
			if req.Method == http.MethodPatch {
				patches++
			}
			obj = rs
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(obj)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))
	defer server.Close()

	client := clientset.NewForConfigOrDie(&restclient.Config{Host: server.URL})
	meta, err := makeTarget(client, "replicaset/web", "default")
	if err != nil {
		t.Fatal(err)
	}
	tc := &targetClient{meta: *meta, clientset: client}
	ctx := context.Background()
	if _, err := tc.IsSelfHealing(ctx); err != nil { // fetches and caches the spec
		t.Fatal(err)
	}
	want := map[string]apiv1.ResourceRequirements{"main": {Requests: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("200m")}}}

	if changed, err := tc.PatchTemplate(ctx, want); err != nil || !changed {
		t.Fatalf("first patch: changed=%v err=%v, want true/nil", changed, err)
	}
	if changed, err := tc.PatchTemplate(ctx, want); err != nil || changed {
		t.Errorf("second patch, same values: changed=%v err=%v, want false/nil", changed, err)
	}
	if patches != 1 {
		t.Errorf("template PATCHes = %d, want 1", patches)
	}
}
