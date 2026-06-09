/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package autoscaler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler/pkg/autoscaler/k8sclient"
	clocktesting "k8s.io/utils/clock/testing"
)

// TestPollAPIServer_RecreateMode is a controller-level integration test
// that spins up a mocked API server, creates a real k8sClient + AutoScaler,
// calls pollAPIServer, and verifies the Deployment template is patched.
func TestPollAPIServer_RecreateMode(t *testing.T) {
	deployPatched := false
	var patchBody []byte

	server := newMockAPIServer(t, mockServerConfig{
		mode:               k8sclient.ResizeModeRecreate,
		deploymentPatched:  &deployPatched,
		deploymentPatchBuf: &patchBody,
	})
	defer server.Close()

	kubeconfig := writeTempKubeconfig(t, server.URL)
	defer os.Remove(kubeconfig)
	client, err := k8sclient.NewK8sClient("default", "deployment/test-dep", kubeconfig, false, k8sclient.ResizeModeRecreate, k8sclient.ResizeFallbackConfig{})
	if err != nil {
		t.Fatalf("NewK8sClient: %v", err)
	}

	cfgJSON := `{"main":{"requests":{"cpu":{"base":"10m","step":"1m","coresPerStep":1}}}}`
	cfg := ScaleConfig{}
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	as := &AutoScaler{
		k8sClient:     client,
		defaultConfig: cfg,
		pollPeriod:    5 * time.Second,
		clock:         clocktesting.NewFakeClock(time.Now()),
		stopCh:        make(chan struct{}),
		resizeMode:    k8sclient.ResizeModeRecreate,
	}

	as.pollAPIServer(context.Background())

	if !deployPatched {
		t.Fatal("deployment template was NOT patched in Recreate mode")
	}
	var patch map[string]interface{}
	if err := json.Unmarshal(patchBody, &patch); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	spec := patch["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	podSpec := template["spec"].(map[string]interface{})
	containers := podSpec["containers"].([]interface{})
	if len(containers) != 1 {
		t.Fatalf("expected 1 container in patch, got %d", len(containers))
	}
	ctr := containers[0].(map[string]interface{})
	if ctr["name"] != "main" {
		t.Errorf("container name = %v, want main", ctr["name"])
	}
	res := ctr["resources"].(map[string]interface{})
	reqs := res["requests"].(map[string]interface{})
	cpu := reqs["cpu"].(string)
	// 4 nodes, 7 cores → base 10m + step 1m * 7 cores = 17m
	if cpu != "17m" {
		t.Errorf("patched CPU request = %v, want 17m", cpu)
	}
}

// TestPollAPIServer_InPlaceMode verifies that in InPlace mode the
// controller resizes running pods via the /resize subresource, leaving the deployment template untouched.
func TestPollAPIServer_InPlaceMode(t *testing.T) {
	deployPatched := false
	resizePatched := false
	var deployPatch []byte

	server := newMockAPIServer(t, mockServerConfig{
		mode:               k8sclient.ResizeModeInPlace,
		deploymentPatched:  &deployPatched,
		deploymentPatchBuf: &deployPatch,
		resizePatched:      &resizePatched,
	})
	defer server.Close()

	kubeconfig := writeTempKubeconfig(t, server.URL)
	defer os.Remove(kubeconfig)
	client, err := k8sclient.NewK8sClient("default", "deployment/test-dep", kubeconfig, false, k8sclient.ResizeModeInPlace, k8sclient.ResizeFallbackConfig{})
	if err != nil {
		t.Fatalf("NewK8sClient: %v", err)
	}

	cfgJSON := `{"main":{"requests":{"cpu":{"base":"10m","step":"1m","coresPerStep":1}}}}`
	cfg := ScaleConfig{}
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	as := &AutoScaler{
		k8sClient:     client,
		defaultConfig: cfg,
		pollPeriod:    5 * time.Second,
		clock:         clocktesting.NewFakeClock(time.Now()),
		stopCh:        make(chan struct{}),
		resizeMode:    k8sclient.ResizeModeInPlace,
	}

	as.pollAPIServer(context.Background())

	if !resizePatched {
		t.Fatal("running pod was not resized")
	}
	if deployPatched {
		t.Fatal("deployment template was patched unexpectedly in InPlace mode")
	}
}

// mockServerConfig configures the mock API server.
type mockServerConfig struct {
	mode               k8sclient.ResizeMode
	deploymentPatched  *bool
	deploymentPatchBuf *[]byte
	resizePatched      *bool
}

// writeTempKubeconfig writes a minimal kubeconfig file that points at the
// given mock server URL so NewK8sClient can be initialised outside a cluster.
func writeTempKubeconfig(t *testing.T, serverURL string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "kubeconfig")
	content := `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: ` + serverURL + `
    insecure-skip-tls-verify: true
  name: mock
contexts:
- context:
    cluster: mock
  name: mock
current-context: mock
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// newMockAPIServer creates a httptest server that emulates enough of the
// Kubernetes API for NewK8sClient + pollAPIServer to succeed.
func newMockAPIServer(t *testing.T, cfg mockServerConfig) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var obj interface{}
		t.Logf("mock %s %s", req.Method, req.URL.Path)

		// --- discovery ---
		switch req.URL.Path {
		case "/api":
			obj = &metav1.APIVersions{Versions: []string{"v1"}}
		case "/api/v1":
			obj = &metav1.APIResourceList{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"list", "get", "patch", "delete"}},
					{Name: "pods/resize", Namespaced: true, Kind: "Pod", Verbs: []string{"patch"}},
					{Name: "nodes", Namespaced: false, Kind: "Node", Verbs: []string{"list"}},
				},
			}
		case "/apis":
			obj = &metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "apps",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "apps/v1", Version: "v1"},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"},
					},
				},
			}
		case "/apis/apps/v1":
			obj = &metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment"},
					{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
					{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				},
			}
		}
		if obj != nil {
			b, _ := json.Marshal(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}

		// --- nodes list ---
		if req.Method == "GET" && req.URL.Path == "/api/v1/nodes" {
			list := apiv1.NodeList{
				Items: []apiv1.Node{
					{Status: apiv1.NodeStatus{Capacity: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("2")}}},
					{Status: apiv1.NodeStatus{Capacity: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("2")}}},
					{Status: apiv1.NodeStatus{Capacity: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("2")}}},
					{Status: apiv1.NodeStatus{Capacity: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("1")}}},
				},
			}
			b, _ := json.Marshal(list)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}

		// --- deployment get ---
		if req.Method == "GET" && req.URL.Path == "/apis/apps/v1/namespaces/default/deployments/test-dep" {
			dep := appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dep", Namespace: "default"},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "test"},
					},
					Template: apiv1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
						Spec: apiv1.PodSpec{
							Containers: []apiv1.Container{{
								Name: "main",
								Resources: apiv1.ResourceRequirements{
									Requests: apiv1.ResourceList{
										apiv1.ResourceCPU: resource.MustParse("10m"),
									},
								},
							}},
						},
					},
				},
			}
			b, _ := json.Marshal(dep)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}

		// --- deployment patch ---
		if req.Method == "PATCH" && req.URL.Path == "/apis/apps/v1/namespaces/default/deployments/test-dep" {
			if cfg.deploymentPatched != nil {
				*cfg.deploymentPatched = true
			}
			if cfg.deploymentPatchBuf != nil {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Errorf("read deployment patch body: %v", err)
				}
				*cfg.deploymentPatchBuf = body
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{}"))
			return
		}

		// --- pod list (for resize) ---
		if req.Method == "GET" && strings.HasPrefix(req.URL.Path, "/api/v1/namespaces/default/pods") {
			list := apiv1.PodList{
				Items: []apiv1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-dep-abc", Namespace: "default",
							UID:    types.UID("pod-1"),
							Labels: map[string]string{"app": "test"},
						},
						Spec: apiv1.PodSpec{
							Containers: []apiv1.Container{{
								Name: "main",
								Resources: apiv1.ResourceRequirements{
									Requests: apiv1.ResourceList{
										apiv1.ResourceCPU: resource.MustParse("10m"),
									},
								},
							}},
						},
						Status: apiv1.PodStatus{Phase: apiv1.PodRunning},
					},
				},
			}
			b, _ := json.Marshal(list)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}

		// --- pod patch resize ---
		if req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize") {
			if cfg.resizePatched != nil {
				*cfg.resizePatched = true
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{}"))
			return
		}

		// --- pod get after resize ---
		if req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/default/pods/test-dep-abc" {
			pod := apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-dep-abc", Namespace: "default",
					UID: types.UID("pod-1"),
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{{
						Name: "main",
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU: resource.MustParse("17m"),
							},
						},
					}},
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
					Conditions: []apiv1.PodCondition{{
						Type:   apiv1.PodResizeInProgress,
						Status: apiv1.ConditionTrue,
					}},
				},
			}
			b, _ := json.Marshal(pod)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}

		t.Logf("mock 404 %s %s", req.Method, req.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
}
