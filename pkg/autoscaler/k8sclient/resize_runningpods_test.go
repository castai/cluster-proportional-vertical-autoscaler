/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package k8sclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

// makePod builds a pod with the given phase and resize conditions.
func makePod(name string, phase v1.PodPhase, conditions []v1.PodCondition, ctrRes v1.ResourceRequirements) v1.Pod {
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "test",
			Name:              name,
			UID:               types.UID(name),
			DeletionTimestamp: nil,
			Labels:            map[string]string{"app": "test"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:      "main",
				Resources: ctrRes,
			}},
		},
		Status: v1.PodStatus{
			Phase:      phase,
			Conditions: conditions,
		},
	}
}

// podListResponse returns a PodList JSON response.
func podListResponse(pods []v1.Pod) []byte {
	pl := v1.PodList{Items: pods}
	b, _ := json.Marshal(pl)
	return b
}

// podResponse returns a single Pod JSON response.
func podResponse(pod *v1.Pod) []byte {
	b, _ := json.Marshal(pod)
	return b
}

// newResizeTestClient creates a k8sClient wired to the given test server.
func newResizeTestClient(server *httptest.Server) *k8sClient {
	client := clientset.NewForConfigOrDie(&restclient.Config{
		Host: server.URL,
		ContentConfig: restclient.ContentConfig{
			GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"},
		},
	})
	return &k8sClient{
		clientset: client,
		// Safe seams so resizeRunningPods can run without a live target object.
		// Tests that need the controller-driven (self-healing) recreate path
		// override selfHealsFn; tests that want to observe the template patch
		// override patchTemplateFn.
		patchTemplateFn: func(map[string]v1.ResourceRequirements) error { return nil },
		selfHealsFn:     func() bool { return false },
	}
}

// TestResizeRunningPods_AllAlreadyOK verifies that when every pod already
// matches the desired resources we get AlreadyOK == pod count.
func TestResizeRunningPods_AllAlreadyOK(t *testing.T) {
	res := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("100m"),
			v1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	pods := []v1.Pod{
		makePod("pod-a", v1.PodRunning, nil, res),
		makePod("pod-b", v1.PodRunning, nil, res),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse(pods))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	desired := map[string]v1.ResourceRequirements{"main": res}
	tracker := newResizeTracker()

	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector, desired, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TargetPods != 2 {
		t.Errorf("TargetPods = %d, want 2", result.TargetPods)
	}
	if result.AlreadyOK != 2 {
		t.Errorf("AlreadyOK = %d, want 2", result.AlreadyOK)
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0", result.Applied)
	}
}

// TestResizeRunningPods_InProgress verifies that a pod already at desired
// spec but showing InProgress is counted correctly via the no-patch path.
func TestResizeRunningPods_InProgress(t *testing.T) {
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizeInProgress,
		Status: v1.ConditionTrue,
	}}, newRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0 (no patch needed)", result.Applied)
	}
	if result.InProgress != 1 {
		t.Errorf("InProgress = %d, want 1", result.InProgress)
	}
}

// TestResizeRunningPods_Deferred verifies Deferred counting via the no-patch path.
func TestResizeRunningPods_Deferred(t *testing.T) {
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: v1.PodReasonDeferred,
	}}, newRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0 (no patch needed)", result.Applied)
	}
	if result.Deferred != 1 {
		t.Errorf("Deferred = %d, want 1", result.Deferred)
	}
}

// TestResizeRunningPods_InfeasibleTracksGrace verifies that an Infeasible
// pod is tracked and NOT deleted before the grace period expires. Because
// classification is now async (no GET-after-PATCH), this test simulates
// two poll cycles: the first patches, the second classifies via the no-patch
// path once conditions have appeared.
func TestResizeRunningPods_InfeasibleTracksGrace(t *testing.T) {
	oldRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)
	infeasiblePod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: v1.PodReasonInfeasible,
	}}, newRes)

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			p := pod
			p.Spec.Containers = append([]v1.Container(nil), pod.Spec.Containers...)
			p.Spec.Containers[0].Resources = newRes
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&p))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	fallback := FallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}

	// First cycle: pod needs a patch. We only count Applied; classification
	// happens on the next cycle once conditions appear.
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
	if deleted {
		t.Error("pod was deleted before grace period expired")
	}

	// Simulate the next poll: kubelet has now written the Infeasible
	// condition. The pod spec already matches desired, so the no-patch
	// path classifies it. Pre-seed tracker so grace has elapsed.
	pod.Spec.Containers = append([]v1.Container(nil), pod.Spec.Containers...)
	pod.Spec.Containers[0].Resources = newRes
	pod.Status.Conditions = infeasiblePod.Status.Conditions
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

	deleted = false
	result, err = k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Infeasible != 1 {
		t.Errorf("Infeasible = %d, want 1", result.Infeasible)
	}
	if result.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", result.Evicted)
	}
	if !deleted {
		t.Error("pod was NOT deleted after grace period expired")
	}
}

// TestResizeRunningPods_SkipTerminalAndDeleting verifies pods in terminal
// phases or with a DeletionTimestamp are ignored.
func TestResizeRunningPods_SkipTerminalAndDeleting(t *testing.T) {
	res := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	now := metav1.Now()
	pods := []v1.Pod{
		makePod("pod-running", v1.PodRunning, nil, res),
		makePod("pod-succeeded", v1.PodSucceeded, nil, res),
		makePod("pod-failed", v1.PodFailed, nil, res),
		func() v1.Pod {
			p := makePod("pod-deleting", v1.PodRunning, nil, res)
			p.DeletionTimestamp = &now
			return p
		}(),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse(pods))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			var p v1.Pod
			for i := range pods {
				if pods[i].Name == "pod-running" {
					p = pods[i]
					p.Spec.Containers = append([]v1.Container(nil), pods[i].Spec.Containers...)
					p.Spec.Containers[0].Resources = newRes
					break
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&p))
		case req.Method == "GET" && strings.HasPrefix(req.URL.Path, "/api/v1/namespaces/test/pods/"):
			var p v1.Pod
			for i := range pods {
				if pods[i].Name == "pod-running" {
					p = pods[i]
					p.Spec.Containers = append([]v1.Container(nil), pods[i].Spec.Containers...)
					p.Spec.Containers[0].Resources = newRes
					break
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&p))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TargetPods != 4 {
		t.Errorf("TargetPods = %d, want 4", result.TargetPods)
	}
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1 (only running pod)", result.Applied)
	}
}

// TestResizeRunningPods_Transient404 verifies that a 404 during patch is
// treated as transient (not counted as an error).
func TestResizeRunningPods_Transient404(t *testing.T) {
	oldRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Errors != 0 {
		t.Errorf("Errors = %d, want 0", result.Errors)
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0", result.Applied)
	}
}

// TestResizeRunningPods_MaxPodsPerCycle verifies that MaxPodsPerCycle
// limits fallback deletes in a single cycle.
func TestResizeRunningPods_MaxPodsPerCycle(t *testing.T) {
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	// Pods already have desired spec but are Infeasible so the no-patch
	// path classifies them and the fallback can fire.
	podA := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Infeasible",
	}}, newRes)
	podB := makePod("pod-b", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Infeasible",
	}}, newRes)

	deleteCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{podA, podB}))
		case req.Method == "DELETE" && strings.HasPrefix(req.URL.Path, "/api/v1/namespaces/test/pods/"):
			deleteCount++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	// Pre-seed tracker so both pods are past grace period.
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)
	tracker.notResizedSince[types.UID("pod-b")] = time.Now().Add(-10 * time.Minute)

	fallback := FallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", result.Evicted)
	}
	if deleteCount != 1 {
		t.Errorf("deleteCount = %d, want 1", deleteCount)
	}
}

// TestResizeRunningPods_NoPatchButInfeasible verifies the bug-fix where
// pods whose spec already matches desired but are Infeasible from a
// previous cycle are still tracked and can be fallback-deleted.
func TestResizeRunningPods_NoPatchButInfeasible(t *testing.T) {
	res := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Infeasible",
	}}, res)

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

	fallback := FallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Infeasible != 1 {
		t.Errorf("Infeasible = %d, want 1", result.Infeasible)
	}
	if result.AlreadyOK != 0 {
		t.Errorf("AlreadyOK = %d, want 0 (pod is Infeasible, not OK)", result.AlreadyOK)
	}
	if !deleted {
		t.Error("pod was NOT deleted after grace period expired")
	}
}

// TestResizeRunningPods_PendingPodIncluded verifies that Pending pods are
// included for convergence. Here the pod already matches desired spec and
// shows InProgress, so the no-patch path classifies it.
func TestResizeRunningPods_PendingPodIncluded(t *testing.T) {
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodPending, []v1.PodCondition{{
		Type:   v1.PodResizeInProgress,
		Status: v1.ConditionTrue,
	}}, newRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0 (no patch needed)", result.Applied)
	}
	if result.InProgress != 1 {
		t.Errorf("InProgress = %d, want 1", result.InProgress)
	}
}

// TestResizeRunningPods_AsyncClassification verifies that conditions
// written asynchronously by the kubelet are detected on the *next* poll
// cycle via the no-patch path, not via a synchronous GET after PATCH.
func TestResizeRunningPods_AsyncClassification(t *testing.T) {
	oldRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			p := pod
			p.Spec.Containers = append([]v1.Container(nil), pod.Spec.Containers...)
			p.Spec.Containers[0].Resources = newRes
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&p))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	// First cycle: patch accepted, but no conditions yet → Applied=1.
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
	if result.InProgress != 0 {
		t.Errorf("InProgress = %d, want 0 (conditions not visible yet)", result.InProgress)
	}

	// Second cycle: kubelet has written InProgress. Pod spec already matches
	// desired, so the no-patch path classifies it.
	pod.Spec.Containers = append([]v1.Container(nil), pod.Spec.Containers...)
	pod.Spec.Containers[0].Resources = newRes
	pod.Status.Conditions = []v1.PodCondition{{
		Type:   v1.PodResizeInProgress,
		Status: v1.ConditionTrue,
	}}

	result, err = k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, FallbackConfig{}, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0 (no patch needed)", result.Applied)
	}
	if result.InProgress != 1 {
		t.Errorf("InProgress = %d, want 1", result.InProgress)
	}
}

// TestResizeRunningPods_PartialFailure_OneOfMany verifies that when one pod in
// a multi-pod workload cannot be resized within the grace period, it is
// recreated via the fallback while the OTHER pods are resized in place and
// left running. The stuck pod must not block or disturb the healthy ones, and
// the template must be patched exactly once (before the recreate).
func TestResizeRunningPods_PartialFailure_OneOfMany(t *testing.T) {
	oldRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")}}
	newRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")}}

	// pod-a and pod-c still need a resize and will accept it in place.
	podA := makePod("pod-a", v1.PodRunning, nil, oldRes)
	podC := makePod("pod-c", v1.PodRunning, nil, oldRes)
	// pod-b is already at the desired spec but stuck Infeasible from a prior
	// cycle — it is the "one of many" that fails to resize within the period.
	podB := makePod("pod-b", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Infeasible",
	}}, newRes)

	inProgress := func(name string) *v1.Pod {
		p := makePod(name, v1.PodRunning, []v1.PodCondition{{
			Type:   v1.PodResizeInProgress,
			Status: v1.ConditionTrue,
		}}, newRes)
		return &p
	}

	var deleted []string
	templatePatches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{podA, podB, podC}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			// The patched pod is returned and classified by the code.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if strings.Contains(req.URL.Path, "/pod-a/resize") {
				w.Write(podResponse(inProgress("pod-a")))
			} else if strings.Contains(req.URL.Path, "/pod-c/resize") {
				w.Write(podResponse(inProgress("pod-c")))
			} else {
				w.Write(podResponse(&podA))
			}
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podResponse(inProgress("pod-a")))
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-c":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podResponse(inProgress("pod-c")))
		case req.Method == "DELETE" && strings.HasPrefix(req.URL.Path, "/api/v1/namespaces/test/pods/"):
			deleted = append(deleted, req.URL.Path)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	k8scli.patchTemplateFn = func(map[string]v1.ResourceRequirements) error {
		templatePatches++
		return nil
	}
	// selfHealsFn defaults to false (bare RS / OnDelete DS): manual-delete path.

	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-b")] = time.Now().Add(-10 * time.Minute) // past grace

	fallback := FallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.TargetPods != 3 {
		t.Errorf("TargetPods = %d, want 3", result.TargetPods)
	}
	if result.Applied != 2 {
		t.Errorf("Applied = %d, want 2 (pod-a and pod-c resized in place)", result.Applied)
	}
	if result.InProgress != 2 {
		t.Errorf("InProgress = %d, want 2", result.InProgress)
	}
	if result.Infeasible != 1 {
		t.Errorf("Infeasible = %d, want 1 (pod-b)", result.Infeasible)
	}
	if result.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1 (only pod-b)", result.Evicted)
	}
	if len(deleted) != 1 || !strings.HasSuffix(deleted[0], "/pod-b") {
		t.Errorf("deleted = %v, want exactly [pod-b]", deleted)
	}
	if templatePatches != 1 {
		t.Errorf("templatePatches = %d, want 1 (patched once, before the recreate)", templatePatches)
	}
}

// TestResizeRunningPods_FallbackSelfHealingNoDelete verifies that for a
// self-healing target (Deployment / RollingUpdate DaemonSet) the fallback
// patches the template and lets the controller recreate the pod, WITHOUT a
// manual delete that would bypass maxUnavailable / PodDisruptionBudgets.
func TestResizeRunningPods_FallbackSelfHealingNoDelete(t *testing.T) {
	newRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")}}
	podA := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Infeasible",
	}}, newRes)

	deleted := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{podA}))
		case req.Method == "DELETE":
			deleted++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	templatePatches := 0
	k8scli.patchTemplateFn = func(map[string]v1.ResourceRequirements) error {
		templatePatches++
		return nil
	}
	k8scli.selfHealsFn = func() bool { return true } // Deployment / RollingUpdate DS

	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

	fallback := FallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if templatePatches != 1 {
		t.Errorf("templatePatches = %d, want 1", templatePatches)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (self-healing target must not be manually deleted)", deleted)
	}
	if result.RecreateTriggered != 1 {
		t.Errorf("RecreateTriggered = %d, want 1 (recreate handed to the controller)", result.RecreateTriggered)
	}
	if result.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0 (no direct delete for a self-healing target)", result.Evicted)
	}
}

// TestResizeRunningPods_PersistentDeferredRecreated verifies the unified
// fallback rule: a pod stuck Deferred (not just Infeasible) past the grace
// period is recreated. Deferred is the common "can't resize up right now"
// outcome under node pressure, so an Infeasible-only fallback would rarely
// fire when it is actually needed.
func TestResizeRunningPods_PersistentDeferredRecreated(t *testing.T) {
	newRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")}}
	// Spec already at desired (no-patch branch) but stuck Deferred.
	pod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Deferred",
	}}, newRes)

	deleteCount := 0
	templatePatches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleteCount++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	k8scli.patchTemplateFn = func(map[string]v1.ResourceRequirements) error {
		templatePatches++
		return nil
	}
	// selfHealsFn defaults to false: manual-delete path.

	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute) // past grace

	fallback := FallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Deferred != 1 {
		t.Errorf("Deferred = %d, want 1", result.Deferred)
	}
	if result.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1 (persistent Deferred should be recreated)", result.Evicted)
	}
	if deleteCount != 1 {
		t.Errorf("deleteCount = %d, want 1", deleteCount)
	}
	if templatePatches != 1 {
		t.Errorf("templatePatches = %d, want 1", templatePatches)
	}
}

// TestResizeRunningPods_TransientDeferredNotRecreated verifies the grace
// period protects a transient Deferred: a pod that has only just gone
// Deferred (well within grace) is left alone to let the kubelet resolve it in
// place, and is NOT recreated.
func TestResizeRunningPods_TransientDeferredNotRecreated(t *testing.T) {
	newRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")}}
	pod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Deferred",
	}}, newRes)

	deleteCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "DELETE":
			deleteCount++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	k8scli := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker() // not pre-seeded: first time seen, age ~0

	fallback := FallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Deferred != 1 {
		t.Errorf("Deferred = %d, want 1", result.Deferred)
	}
	if result.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0 (transient Deferred must not be recreated)", result.Evicted)
	}
	if deleteCount != 0 {
		t.Errorf("deleteCount = %d, want 0", deleteCount)
	}
	// And the pod is now tracked, so a later cycle past grace can act on it.
	if _, ok := tracker.notResizedSince[types.UID("pod-a")]; !ok {
		t.Errorf("expected pod-a to be tracked as not resized")
	}
}
