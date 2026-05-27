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
	return &k8sClient{clientset: client}
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

// TestResizeRunningPods_InProgress verifies that a pod whose patch is
// accepted and whose conditions show InProgress is counted correctly.
func TestResizeRunningPods_InProgress(t *testing.T) {
	oldRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)
	updatedPod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizeInProgress,
		Status: v1.ConditionTrue,
	}}, oldRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			// Accept patch — update spec in the returned pod.
			p := pod
			p.Spec.Containers = append([]v1.Container(nil), pod.Spec.Containers...)
			p.Spec.Containers[0].Resources = newRes
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&p))
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&updatedPod))
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
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
	if result.InProgress != 1 {
		t.Errorf("InProgress = %d, want 1", result.InProgress)
	}
}

// TestResizeRunningPods_Deferred verifies Deferred counting.
func TestResizeRunningPods_Deferred(t *testing.T) {
	oldRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)
	updatedPod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Deferred",
	}}, oldRes)

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
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&updatedPod))
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
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
	if result.Deferred != 1 {
		t.Errorf("Deferred = %d, want 1", result.Deferred)
	}
}

// TestResizeRunningPods_InfeasibleTracksGrace verifies that an Infeasible
// pod is tracked and NOT deleted before the grace period expires.
func TestResizeRunningPods_InfeasibleTracksGrace(t *testing.T) {
	oldRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)
	updatedPod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
		Type:   v1.PodResizePending,
		Reason: "Infeasible",
	}}, oldRes)

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			// Deep-copy the containers slice so the original `pod` is
			// not mutated by the test server (Go struct copy shares
			// the backing array).
			p := pod
			p.Spec.Containers = append([]v1.Container(nil), pod.Spec.Containers...)
			p.Spec.Containers[0].Resources = newRes
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&p))
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&updatedPod))
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

	result, err := k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Infeasible != 1 {
		t.Errorf("Infeasible = %d, want 1", result.Infeasible)
	}
	if deleted {
		t.Error("pod was deleted before grace period expired")
	}
	// Simulate a second poll after grace period has elapsed.
	tracker.infeasibleSeen[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

	deleted = false
	result, err = k8scli.resizeRunningPods(context.Background(), "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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
	tracker.infeasibleSeen[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)
	tracker.infeasibleSeen[types.UID("pod-b")] = time.Now().Add(-10 * time.Minute)

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
	tracker.infeasibleSeen[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

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
// resized in-place so they start with the correct resources.
func TestResizeRunningPods_PendingPodIncluded(t *testing.T) {
	oldRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
	}
	newRes := v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
	}

	pod := makePod("pod-a", v1.PodPending, nil, oldRes)
	updatedPod := makePod("pod-a", v1.PodPending, []v1.PodCondition{{
		Type:   v1.PodResizeInProgress,
		Status: v1.ConditionTrue,
	}}, oldRes)

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
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&updatedPod))
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
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
	if result.InProgress != 1 {
		t.Errorf("InProgress = %d, want 1", result.InProgress)
	}
}
