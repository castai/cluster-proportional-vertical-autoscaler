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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

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

func TestUpdatePodResources_DryRun(t *testing.T) {
	client := clientset.NewForConfigOrDie(&restclient.Config{
		Host: "http://localhost:65535", // won't be called in dry-run mode
	})

	k8scli := &k8sClient{
		clientset: client,
		dryRun:    true,
		target: &targetSpec{
			Kind:         "Deployment",
			Name:         "test-dep",
			Namespace:    "default",
			GroupVersion: "apps/v1",
		},
	}

	req := map[string]apiv1.ResourceRequirements{"app": {}}
	if err := k8scli.UpdatePodResources(req); err != nil {
		t.Errorf("expected nil error in dry-run mode, got: %v", err)
	}
}

func TestGetTargetSelector(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		targetName string
		selector  *metav1.LabelSelector
	}{
		{
			name:       "deployment",
			kind:       "Deployment",
			targetName: "test-dep",
			selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
		},
		{
			name:       "daemonset",
			kind:       "DaemonSet",
			targetName: "test-ds",
			selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "ds"}},
		},
		{
			name:       "replicaset",
			kind:       "ReplicaSet",
			targetName: "test-rs",
			selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "rs"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var obj interface{}
				switch req.URL.Path {
				case "/apis/apps/v1" + "/namespaces/default/" + strings.ToLower(tc.kind) + "s/" + tc.targetName:
					obj = map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":      tc.targetName,
							"namespace": "default",
						},
						"spec": map[string]interface{}{
							"selector": tc.selector,
						},
					}
				default:
					w.WriteHeader(http.StatusNotFound)
					return
				}
				output, _ := json.Marshal(obj)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(output)
			}))
			defer server.Close()

			client := clientset.NewForConfigOrDie(&restclient.Config{
				Host: server.URL,
				ContentConfig: restclient.ContentConfig{
					GroupVersion: &schema.GroupVersion{Group: "apps", Version: "v1"},
				},
			})

			k8scli := &k8sClient{
				clientset: client,
				target: &targetSpec{
					Kind:         tc.kind,
					Name:         tc.targetName,
					Namespace:    "default",
					GroupVersion: "apps/v1",
				},
			}

			sel, err := k8scli.getTargetSelector()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expected := metav1.FormatLabelSelector(tc.selector)
			if sel != expected {
				t.Errorf("expected selector %q, got %q", expected, sel)
			}
		})
	}
}

func TestUpdatePodResources_PatchesPods(t *testing.T) {
	patched := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/api":
			obj := &metav1.APIVersions{Versions: []string{"v1"}}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/api/v1":
			obj := metav1.APIResourceList{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Namespaced: true, Kind: "Pod"},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis":
			obj := &metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{Name: "apps", Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "apps/v1", Version: "v1"}}, PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"}},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis/apps/v1":
			obj := metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment"},
					{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
					{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis/apps/v1/namespaces/default/deployments/test-dep":
			obj := map[string]interface{}{
				"metadata": map[string]interface{}{
					"name":      "test-dep",
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": "test"}},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		case "/api/v1/namespaces/default/pods":
			if req.URL.Query().Get("labelSelector") != "app=test" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			obj := map[string]interface{}{
				"items": []map[string]interface{}{
					{
						"metadata": map[string]interface{}{
							"name":      "running-pod",
							"namespace": "default",
							"labels":    map[string]interface{}{"app": "test"},
						},
						"status": map[string]interface{}{"phase": "Running"},
					},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		case "/api/v1/namespaces/default/pods/running-pod":
			if req.Method != http.MethodPatch {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			patched = true
			pod := map[string]interface{}{
				"metadata": map[string]interface{}{
					"name":      "running-pod",
					"namespace": "default",
					"labels":    map[string]interface{}{"app": "test"},
				},
				"status": map[string]interface{}{"phase": "Running"},
			}
			output, _ := json.Marshal(pod)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		default:
			t.Logf("unexpected request: %s %s", req.Method, req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := clientset.NewForConfigOrDie(&restclient.Config{
		Host: server.URL,
		ContentConfig: restclient.ContentConfig{
			GroupVersion: &schema.GroupVersion{Group: "apps", Version: "v1"},
		},
	})

	k8scli := &k8sClient{
		clientset: client,
		target: &targetSpec{
			Kind:         "Deployment",
			Name:         "test-dep",
			Namespace:    "default",
			GroupVersion: "apps/v1",
		},
	}

	req := map[string]apiv1.ResourceRequirements{"app": {}}
	if err := k8scli.UpdatePodResources(req); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if !patched {
		t.Error("expected pod to be patched")
	}

	if k8scli.inPlaceAvailable == nil || !*k8scli.inPlaceAvailable {
		t.Error("expected inPlaceAvailable to be true")
	}
}

func TestUpdatePodResources_SkipsTerminalPods(t *testing.T) {
	var patchCalls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/api":
			obj := &metav1.APIVersions{Versions: []string{"v1"}}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/api/v1":
			obj := metav1.APIResourceList{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Namespaced: true, Kind: "Pod"},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis":
			obj := &metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{Name: "apps", Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "apps/v1", Version: "v1"}}, PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"}},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis/apps/v1":
			obj := metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment"},
					{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
					{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis/apps/v1/namespaces/default/deployments/test-dep":
			obj := map[string]interface{}{
				"metadata": map[string]interface{}{
					"name":      "test-dep",
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": "test"}},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		case "/api/v1/namespaces/default/pods":
			if req.URL.Query().Get("labelSelector") != "app=test" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			deletionTimestamp := "2024-01-01T00:00:00Z"
			obj := map[string]interface{}{
				"items": []map[string]interface{}{
					{
						"metadata": map[string]interface{}{
							"name":               "terminating-pod",
							"namespace":          "default",
							"labels":             map[string]interface{}{"app": "test"},
							"deletionTimestamp":  deletionTimestamp,
						},
						"status": map[string]interface{}{"phase": "Running"},
					},
					{
						"metadata": map[string]interface{}{
							"name":      "succeeded-pod",
							"namespace": "default",
							"labels":    map[string]interface{}{"app": "test"},
						},
						"status": map[string]interface{}{"phase": "Succeeded"},
					},
					{
						"metadata": map[string]interface{}{
							"name":      "running-pod",
							"namespace": "default",
							"labels":    map[string]interface{}{"app": "test"},
						},
						"status": map[string]interface{}{"phase": "Running"},
					},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		case "/api/v1/namespaces/default/pods/running-pod":
			if req.Method != http.MethodPatch {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			patchCalls = append(patchCalls, "running-pod")
			pod := map[string]interface{}{
				"metadata": map[string]interface{}{
					"name":      "running-pod",
					"namespace": "default",
					"labels":    map[string]interface{}{"app": "test"},
				},
				"status": map[string]interface{}{"phase": "Running"},
			}
			output, _ := json.Marshal(pod)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := clientset.NewForConfigOrDie(&restclient.Config{
		Host: server.URL,
		ContentConfig: restclient.ContentConfig{
			GroupVersion: &schema.GroupVersion{Group: "apps", Version: "v1"},
		},
	})

	k8scli := &k8sClient{
		clientset: client,
		target: &targetSpec{
			Kind:         "Deployment",
			Name:         "test-dep",
			Namespace:    "default",
			GroupVersion: "apps/v1",
		},
	}

	req := map[string]apiv1.ResourceRequirements{"app": {}}
	if err := k8scli.UpdatePodResources(req); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if len(patchCalls) != 1 || patchCalls[0] != "running-pod" {
		t.Errorf("expected only running-pod to be patched, got: %v", patchCalls)
	}
}

func TestUpdatePodResources_DisablesOnUnsupportedCluster(t *testing.T) {
	var patchCalls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/api":
			obj := &metav1.APIVersions{Versions: []string{"v1"}}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/api/v1":
			obj := metav1.APIResourceList{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Namespaced: true, Kind: "Pod"},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis":
			obj := &metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{Name: "apps", Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "apps/v1", Version: "v1"}}, PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"}},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis/apps/v1":
			obj := metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment"},
					{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
					{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		case "/apis/apps/v1/namespaces/default/deployments/test-dep":
			obj := map[string]interface{}{
				"metadata": map[string]interface{}{
					"name":      "test-dep",
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": "test"}},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		case "/api/v1/namespaces/default/pods":
			if req.URL.Query().Get("labelSelector") != "app=test" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			obj := map[string]interface{}{
				"items": []map[string]interface{}{
					{
						"metadata": map[string]interface{}{
							"name":      "running-pod",
							"namespace": "default",
							"labels":    map[string]interface{}{"app": "test"},
						},
						"status": map[string]interface{}{"phase": "Running"},
					},
				},
			}
			output, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(output)

		case "/api/v1/namespaces/default/pods/running-pod":
			patchCalls = append(patchCalls, req.URL.Path)
			// Return 422 Unprocessable Entity - indicates in-place resize not supported
			statusErr := metav1.Status{
				TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
				Status:   metav1.StatusFailure,
				Message:  "Pod is invalid",
				Reason:   metav1.StatusReasonInvalid,
				Code:     422,
				Details: &metav1.StatusDetails{
					Causes: []metav1.StatusCause{
						{Type: metav1.CauseTypeForbidden, Field: "spec.containers[0].resources"},
					},
				},
			}
			output, _ := json.Marshal(statusErr)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			w.Write(output)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := clientset.NewForConfigOrDie(&restclient.Config{
		Host: server.URL,
		ContentConfig: restclient.ContentConfig{
			GroupVersion: &schema.GroupVersion{Group: "apps", Version: "v1"},
		},
	})

	k8scli := &k8sClient{
		clientset: client,
		target: &targetSpec{
			Kind:         "Deployment",
			Name:         "test-dep",
			Namespace:    "default",
			GroupVersion: "apps/v1",
		},
	}

	req := map[string]apiv1.ResourceRequirements{"app": {}}

	// First call should return nil (disables after receiving 422)
	if err := k8scli.UpdatePodResources(req); err != nil {
		t.Fatalf("expected nil error on first call, got: %v", err)
	}

	if k8scli.inPlaceAvailable == nil || *k8scli.inPlaceAvailable {
		t.Error("expected inPlaceAvailable to be false")
	}

	initialPatchCount := len(patchCalls)

	// Second call should skip silently (cached as unavailable)
	if err := k8scli.UpdatePodResources(req); err != nil {
		t.Fatalf("expected nil error on second call, got: %v", err)
	}

	// No additional PATCH requests should have been made
	if len(patchCalls) != initialPatchCount {
		t.Errorf("expected no additional PATCH requests, but got %d total", len(patchCalls))
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
			},
		}
		stable := metav1.APIResourceList{
			GroupVersion: "extensions/v1beta1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment"},
				{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
				{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
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
			obj = &stable
		case "/apis/extensions/v1beta1/namespaces/default/daemonsets/thing":
		case "/apis/extensions/v1beta1/namespaces/default/replicasets/thing":
		case "/apis/extensions/v1beta1/namespaces/default/deployments/thing":
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
	}

	for _, tc := range testCases {
		client := clientset.NewForConfigOrDie(&restclient.Config{
			Host: server.URL,
			ContentConfig: restclient.ContentConfig{
				GroupVersion: &schema.GroupVersion{Group: tc.kind, Version: "extensions/v1beta1"}}})

		target, err := makeTarget(client, tc.target, "default")
		if err != nil {
			t.Fatalf("error making target %q: %v", tc.target, err)
		}
		k8scli := &k8sClient{
			clientset: client,
			target:    target,
		}

		newReqs := map[string]apiv1.ResourceRequirements{}
		newReqs["thing"] = apiv1.ResourceRequirements{
			Requests: map[apiv1.ResourceName]resource.Quantity{},
			Limits:   map[apiv1.ResourceName]resource.Quantity{},
		}
		r := resource.NewQuantity(0, resource.BinarySI)
		r.SetMilli(10)
		newReqs["thing"].Requests[apiv1.ResourceName("cpu")] = *r
		if err := k8scli.UpdateResources(newReqs); err != nil {
			t.Errorf("failed to update resources for target %q: %v", tc.target, err)
		}
	}
}
