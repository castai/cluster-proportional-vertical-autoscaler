/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
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
	"k8s.io/utils/clock"
)

// ---------------------------------------------------------------------------
// resize_test.go helpers
// ---------------------------------------------------------------------------

func mustQty(t *testing.T, s string) resource.Quantity {
	t.Helper()
	return resource.MustParse(s)
}

func reqs(t *testing.T, cpu, mem string) v1.ResourceRequirements {
	return v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    mustQty(t, cpu),
			v1.ResourceMemory: mustQty(t, mem),
		},
	}
}

// ---------------------------------------------------------------------------
// resize_test.go: buildResizePatch tests
// ---------------------------------------------------------------------------

// Verifies that buildResizePatch is a no-op when the pod is already at
// the desired state, and that "1000m" vs "1" don't trigger a spurious
// patch — without this, cpvpa would churn on every poll.
func TestBuildResizePatch_NoOpWhenEqual(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "main", Resources: reqs(t, "1000m", "256Mi")},
			},
		},
	}
	desired := map[string]v1.ResourceRequirements{
		"main": reqs(t, "1", "256Mi"),
	}
	body, need := buildResizePatch(pod, desired)
	if need {
		t.Fatalf("expected no patch needed, got body=%s", string(body))
	}
}

// Verifies that only managed containers appear in the patch, and that the
// patch is a strategic-merge shape (an array keyed by name, not a
// wholesale replacement).
func TestBuildResizePatch_OnlyManagedContainers(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "main", Resources: reqs(t, "100m", "128Mi")},
				{Name: "sidecar", Resources: reqs(t, "50m", "64Mi")},
			},
		},
	}
	desired := map[string]v1.ResourceRequirements{
		"main": reqs(t, "500m", "256Mi"),
		// sidecar intentionally omitted — cpvpa doesn't manage it.
	}
	body, need := buildResizePatch(pod, desired)
	if !need {
		t.Fatal("expected patch to be needed")
	}
	var parsed struct {
		Spec struct {
			Containers []struct {
				Name      string `json:"name"`
				Resources v1.ResourceRequirements
			} `json:"containers"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(parsed.Spec.Containers) != 1 || parsed.Spec.Containers[0].Name != "main" {
		t.Fatalf("expected exactly [main] in patch, got %+v", parsed.Spec.Containers)
	}
}

// Verifies that a pod with extra resource dimensions (e.g. limits) that
// are NOT in the desired config does NOT trigger a perpetual patch.
// This is the fix for the churn bug: cpvpa should only compare keys it
// actually manages and leave everything else alone.
func TestBuildResizePatch_NoOpWhenPodHasExtraDimensions(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "main",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    mustQty(t, "250m"),
						v1.ResourceMemory: mustQty(t, "128Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    mustQty(t, "500m"),
						v1.ResourceMemory: mustQty(t, "256Mi"),
					},
				},
			}},
		},
	}
	// Config only sets requests — cpvpa does not manage limits.
	desired := map[string]v1.ResourceRequirements{
		"main": {
			Requests: v1.ResourceList{
				v1.ResourceCPU:    mustQty(t, "250m"),
				v1.ResourceMemory: mustQty(t, "128Mi"),
			},
		},
	}
	body, need := buildResizePatch(pod, desired)
	if need {
		t.Fatalf("expected no patch needed when pod already satisfies managed keys, got body=%s", string(body))
	}
}

// ---------------------------------------------------------------------------
// resize_test.go: classifyResize tests
// ---------------------------------------------------------------------------

// Pod condition matrix: makes sure we classify each kubelet-reported
// state correctly. This is the part that decides whether cpvpa retries,
// waits, or falls back to delete.
func TestClassifyResize(t *testing.T) {
	cases := []struct {
		name       string
		conditions []v1.PodCondition
		want       resizeStatus
	}{
		{
			name:       "no conditions = OK",
			conditions: nil,
			want:       resizeStatusOK,
		},
		{
			name: "pending Deferred",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizePending, Reason: v1.PodReasonDeferred,
			}},
			want: resizeStatusDeferred,
		},
		{
			name: "pending Infeasible",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizePending, Reason: v1.PodReasonInfeasible,
			}},
			want: resizeStatusInfeasible,
		},
		{
			name: "in progress true",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizeInProgress, Status: v1.ConditionTrue,
			}},
			want: resizeStatusInProgress,
		},
		{
			name: "in progress false (already done)",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizeInProgress, Status: v1.ConditionFalse,
			}},
			want: resizeStatusOK,
		},
		{
			name: "pending wins over in-progress regardless of slice order",
			conditions: []v1.PodCondition{
				{Type: v1.PodResizeInProgress, Status: v1.ConditionTrue},
				{Type: v1.PodResizePending, Reason: v1.PodReasonDeferred},
			},
			want: resizeStatusDeferred,
		},
		{
			name: "in-progress with Error reason is not InProgress",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizeInProgress, Status: v1.ConditionTrue, Reason: v1.PodReasonError,
			}},
			want: resizeStatusOK,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{Status: v1.PodStatus{Conditions: tc.conditions}}
			if got := classifyResize(pod); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// resize_test.go: resizeTracker tests
// ---------------------------------------------------------------------------

// Tracker contract: the first-seen timestamp is stable across repeat calls
// (so the grace period measures continuous time-not-resized, not the most recent
// blip), and clear() resets it — clear() is called when a pod returns to OK,
// so a pod that recovers and later fails again starts a fresh grace window
// rather than being recreated on stale history.
func TestResizeTracker_FirstSeenIsStable(t *testing.T) {
	tr := newResizeTracker()
	uid := types.UID("abc")
	t0 := time.Now()
	first := tr.markNotResized(uid, t0)
	if !first.Equal(t0) {
		t.Fatalf("first call should return now, got %v", first)
	}
	t1 := t0.Add(2 * time.Minute)
	second := tr.markNotResized(uid, t1)
	if !second.Equal(t0) {
		t.Fatalf("second call should still return original timestamp, got %v", second)
	}
	tr.clear(uid)
	third := tr.markNotResized(uid, t1)
	if !third.Equal(t1) {
		t.Fatalf("after clear, should return new timestamp, got %v", third)
	}
}

// Sanity: ObjectMeta UIDs do change across pod recreations, so the
// tracker keyed by UID won't falsely carry state across a fallback
// delete + recreate.
func TestResizeTracker_DistinctUIDs(t *testing.T) {
	tr := newResizeTracker()
	t0 := time.Now()
	tr.markNotResized(types.UID("pod-v1"), t0)
	tr.clear(types.UID("pod-v1")) // fallback deleted it
	got := tr.markNotResized(types.UID("pod-v2"), t0.Add(time.Minute))
	if !got.Equal(t0.Add(time.Minute)) {
		t.Fatalf("new pod UID should get fresh timestamp")
	}
}

// retain drops tracker entries for UIDs not present in live (e.g.
// pods deleted by node drain or scale-down), preventing unbounded growth.
func TestResizeTracker_Retain(t *testing.T) {
	tr := newResizeTracker()
	t0 := time.Now()
	tr.markNotResized(types.UID("pod-a"), t0)
	tr.markNotResized(types.UID("pod-b"), t0)

	// Only pod-a is still in the cluster.
	live := map[types.UID]bool{
		types.UID("pod-a"): true,
	}
	tr.retain(live)

	if _, ok := tr.notResizedSince[types.UID("pod-a")]; !ok {
		t.Fatal("pod-a should still be tracked")
	}
	if _, ok := tr.notResizedSince[types.UID("pod-b")]; ok {
		t.Fatal("pod-b should have been pruned")
	}
}

// ---------------------------------------------------------------------------
// resize_runningpods_test.go helpers
// ---------------------------------------------------------------------------

// fakeResizeTarget implements the resizeTarget interface for testing.
type fakeResizeTarget struct {
	selector  labels.Selector
	namespace string
	selfHeals func(ctx context.Context) bool
	patcher   func(resources map[string]v1.ResourceRequirements) error
}

func (f *fakeResizeTarget) GetPodSelector(ctx context.Context) (labels.Selector, error) {
	return f.selector, nil
}

func (f *fakeResizeTarget) IsSelfHealing(ctx context.Context) bool {
	if f.selfHeals != nil {
		return f.selfHeals(ctx)
	}
	return false
}

func (f *fakeResizeTarget) PatchTemplate(ctx context.Context, resources map[string]v1.ResourceRequirements) error {
	if f.patcher != nil {
		return f.patcher(resources)
	}
	return nil
}

func (f *fakeResizeTarget) Namespace() string {
	return f.namespace
}

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

// newResizeTestClient creates a clientset.Interface wired to the given test server.
func newResizeTestClient(server *httptest.Server) clientset.Interface {
	return clientset.NewForConfigOrDie(&restclient.Config{
		Host: server.URL,
		ContentConfig: restclient.ContentConfig{
			GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"},
		},
	})
}

func resizeWithFakeTarget(
	ctx context.Context,
	client clientset.Interface,
	namespace string,
	selector labels.Selector,
	desired map[string]v1.ResourceRequirements,
	mode ResizeMode,
	fallback ResizeFallbackConfig,
	tracker *resizeTracker,
	selfHeals func(ctx context.Context) bool,
	dryRun bool,
) (resizeResult, error) {
	fake := &fakeResizeTarget{
		selector:  selector,
		namespace: namespace,
		selfHeals: selfHeals,
		patcher:   func(resources map[string]v1.ResourceRequirements) error { return nil },
	}
	r := &podResizer{
		resizeMode:     mode,
		fallbackConfig: fallback,
		dryRun:         dryRun,
		clock:          clock.RealClock{},
		clientset:      client,
		target:         fake,
		tracker:        tracker,
	}
	return r.resizeRunningPods(ctx, desired)
}

// ---------------------------------------------------------------------------
// resize_runningpods_test.go: podResizer tests
// ---------------------------------------------------------------------------

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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	desired := map[string]v1.ResourceRequirements{"main": res}
	tracker := newResizeTracker()

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector, desired, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
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
// pod is tracked and NOT deleted before the grace period expires.
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker,
		func(ctx context.Context) bool { return false },
		false)
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
	result, err = resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker,
		func(ctx context.Context) bool { return false },
		false)
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Errors != 1 {
		t.Errorf("Errors = %d, want 1", result.Errors)
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	// Pre-seed tracker so both pods are past grace period.
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)
	tracker.notResizedSince[types.UID("pod-b")] = time.Now().Add(-10 * time.Minute)

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker,
		func(ctx context.Context) bool { return false },
		false)
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, fallback, tracker,
		func(ctx context.Context) bool { return false },
		false)
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
// included for convergence.
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
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
// cycle via the no-patch path.
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	// First cycle: patch accepted, but no conditions yet → Applied=1.
	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
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

	result, err = resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, tracker,
		func(ctx context.Context) bool { return false },
		false)
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
// left running.
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-b")] = time.Now().Add(-10 * time.Minute) // past grace

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	patcher := func(resources map[string]v1.ResourceRequirements) error { templatePatches++; return nil }
	fake := &fakeResizeTarget{
		selector:  selector,
		namespace: "test",
		selfHeals: func(ctx context.Context) bool { return false },
		patcher:   patcher,
	}
	r := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		dryRun:         false,
		clock:          clock.RealClock{},
		clientset:      client,
		target:         fake,
		tracker:        tracker,
	}
	result, err := r.resizeRunningPods(context.Background(), map[string]v1.ResourceRequirements{"main": newRes})
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
// manual delete.
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	patcher := func(resources map[string]v1.ResourceRequirements) error { return nil }
	fake := &fakeResizeTarget{
		selector:  selector,
		namespace: "test",
		selfHeals: func(ctx context.Context) bool { return true }, // Deployment / RollingUpdate DS
		patcher:   patcher,
	}
	r := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		dryRun:         false,
		clock:          clock.RealClock{},
		clientset:      client,
		target:         fake,
		tracker:        tracker,
	}
	result, err := r.resizeRunningPods(context.Background(), map[string]v1.ResourceRequirements{"main": newRes})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RecreateTriggered != 1 {
		t.Errorf("RecreateTriggered = %d, want 1 (recreate handed to the controller)", result.RecreateTriggered)
	}
	if result.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0 (no direct delete for a self-healing target)", result.Evicted)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (self-healing target must not be manually deleted)", deleted)
	}
}

// TestResizeRunningPods_PersistentDeferredRecreated verifies the unified
// fallback rule: a pod stuck Deferred past the grace period is recreated.
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute) // past grace

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	patcher := func(resources map[string]v1.ResourceRequirements) error { templatePatches++; return nil }
	fake := &fakeResizeTarget{
		selector:  selector,
		namespace: "test",
		selfHeals: func(ctx context.Context) bool { return false },
		patcher:   patcher,
	}
	resizer := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		dryRun:         false,
		clock:          clock.RealClock{},
		clientset:      client,
		target:         fake,
		tracker:        tracker,
	}
	result, err := resizer.resizeRunningPods(context.Background(), map[string]v1.ResourceRequirements{"main": newRes})
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
// period protects a transient Deferred.
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

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker() // not pre-seeded: first time seen, age ~0

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	fake := &fakeResizeTarget{
		selector:  selector,
		namespace: "test",
		selfHeals: func(ctx context.Context) bool { return false },
		patcher:   func(resources map[string]v1.ResourceRequirements) error { return nil },
	}
	resizer := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		dryRun:         false,
		clock:          clock.RealClock{},
		clientset:      client,
		target:         fake,
		tracker:        tracker,
	}
	result, err := resizer.resizeRunningPods(context.Background(), map[string]v1.ResourceRequirements{"main": newRes})
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

// TestResizeRunningPods_InvalidPatchNoPanic is a regression test: a synchronous
// Invalid (HTTP 422) rejection from the /resize patch must not panic the loop.
func TestResizeRunningPods_InvalidPatchNoPanic(t *testing.T) {
	oldRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")}}
	newRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")}}

	podA := makePod("pod-a", v1.PodRunning, nil, oldRes) // /resize rejected with 422
	podB := makePod("pod-b", v1.PodRunning, nil, oldRes) // /resize succeeds

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{podA, podB}))
		case req.Method == "PATCH" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a/resize":
			w.WriteHeader(http.StatusUnprocessableEntity) // 422 -> apierrors.IsInvalid
		case req.Method == "PATCH" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-b/resize":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(podResponse(&podB))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()

	fake := &fakeResizeTarget{
		selector:  selector,
		namespace: "test",
		selfHeals: func(ctx context.Context) bool { return false },
		patcher:   func(resources map[string]v1.ResourceRequirements) error { return nil },
	}
	resizer := &podResizer{
		resizeMode:     ResizeModeInPlace,
		fallbackConfig: ResizeFallbackConfig{},
		dryRun:         false,
		clock:          clock.RealClock{},
		clientset:      client,
		target:         fake,
		tracker:        tracker,
	}
	result, err := resizer.resizeRunningPods(context.Background(), map[string]v1.ResourceRequirements{"main": newRes})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// pod-a's 422 is skipped (not counted as an error); pod-b still resized.
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1 (pod-b processed after pod-a was rejected)", result.Applied)
	}
	if result.Errors != 1 {
		t.Errorf("Errors = %d, want 1", result.Errors)
	}
}

// TestResizeRunningPods_DryRunNoFallbackDelete verifies that in dry-run mode
// an Infeasible pod past its grace period does not trigger any delete action.
func TestResizeRunningPods_DryRunNoFallbackDelete(t *testing.T) {
	newRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")}}

	pod := makePod("pod-a", v1.PodRunning, []v1.PodCondition{{
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
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newResizeTestClient(server)
	selector := labels.SelectorFromSet(map[string]string{"app": "test"})
	tracker := newResizeTracker()
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}

	// Pre-seed tracker so grace has already elapsed.
	tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-10 * time.Minute)

	result, err := resizeWithFakeTarget(context.Background(), client, "test", selector,
		map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, tracker,
		func(ctx context.Context) bool { return false },
		true) // dryRun = true
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Infeasible != 1 {
		t.Errorf("Infeasible = %d, want 1", result.Infeasible)
	}
	if deleted {
		t.Error("pod was deleted in dry-run mode")
	}
	// Tracker entry should still be present since we didn't actually evict.
	if _, ok := tracker.notResizedSince[types.UID("pod-a")]; !ok {
		t.Errorf("expected pod-a tracker entry to be retained in dry-run")
	}
}
