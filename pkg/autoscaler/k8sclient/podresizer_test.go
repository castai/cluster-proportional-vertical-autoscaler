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
// helpers
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

// cpuReq builds a ResourceRequirements with only a CPU request.
func cpuReq(cpu string) v1.ResourceRequirements {
	return v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse(cpu)},
	}
}

// Pod resize condition helpers.
func infeasibleConds() []v1.PodCondition {
	return []v1.PodCondition{{Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonInfeasible}}
}
func deferredConds() []v1.PodCondition {
	return []v1.PodCondition{{Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonDeferred}}
}
func inProgressConds() []v1.PodCondition {
	return []v1.PodCondition{{Type: v1.PodResizeInProgress, Status: v1.ConditionTrue}}
}

// stdFallback is the ResizeFallbackConfig used by most tests:
// 5-minute grace, at most 1 pod disrupted per cycle, delete method.
var stdFallback = ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}

// newPodListServer builds a test server that serves pods on GET
// /api/v1/namespaces/test/pods and returns 404 for all other requests.
func newPodListServer(pods []v1.Pod) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods" {
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse(pods))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

// resizeRig bundles the per-test client, selector, and tracker that appear at
// the top of almost every resizeRunningPods test.
type resizeRig struct {
	t        *testing.T
	client   clientset.Interface
	selector labels.Selector
	tracker  *resizeTracker
}

func newResizeRig(t *testing.T, server *httptest.Server) *resizeRig {
	t.Helper()
	return &resizeRig{
		t:        t,
		client:   newResizeTestClient(server),
		selector: labels.SelectorFromSet(map[string]string{"app": "test"}),
		tracker:  newResizeTracker(),
	}
}

// pastGrace pre-seeds the tracker so the named pods appear past the 5-minute
// fallback grace period (10 minutes ago).
func (rig *resizeRig) pastGrace(uids ...string) {
	for _, uid := range uids {
		rig.tracker.notResizedSince[types.UID(uid)] = time.Now().Add(-10 * time.Minute)
	}
}

// run invokes resizeRunningPods via a fakeResizeTarget and fatals on error.
func (rig *resizeRig) run(desired map[string]v1.ResourceRequirements, mode ResizeMode, fallback ResizeFallbackConfig, selfHeals bool) resizeResult {
	rig.t.Helper()
	result, err := resizeWithFakeTarget(context.Background(), rig.client, "test", rig.selector, desired, mode, fallback, rig.tracker,
		func(ctx context.Context) bool { return selfHeals }, false)
	if err != nil {
		rig.t.Fatalf("unexpected error: %v", err)
	}
	return result
}

// runDry is like run but with dryRun = true.
func (rig *resizeRig) runDry(desired map[string]v1.ResourceRequirements, mode ResizeMode, fallback ResizeFallbackConfig, selfHeals bool) resizeResult {
	rig.t.Helper()
	result, err := resizeWithFakeTarget(context.Background(), rig.client, "test", rig.selector, desired, mode, fallback, rig.tracker,
		func(ctx context.Context) bool { return selfHeals }, true)
	if err != nil {
		rig.t.Fatalf("unexpected error: %v", err)
	}
	return result
}

// ---------------------------------------------------------------------------
// buildResizePatch tests
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
// classifyResize tests
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
				Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonDeferred,
			}},
			want: resizeStatusDeferred,
		},
		{
			name: "pending Infeasible",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonInfeasible,
			}},
			want: resizeStatusInfeasible,
		},
		{
			name: "pending false",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizePending, Status: v1.ConditionFalse, Reason: v1.PodReasonDeferred,
			}},
			want: resizeStatusOK,
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
				{Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonDeferred},
			},
			want: resizeStatusDeferred,
		},
		{
			name: "pending false does not win over in-progress",
			conditions: []v1.PodCondition{
				{Type: v1.PodResizeInProgress, Status: v1.ConditionTrue},
				{Type: v1.PodResizePending, Status: v1.ConditionFalse, Reason: v1.PodReasonDeferred},
			},
			want: resizeStatusInProgress,
		},
		{
			name: "in-progress with Error reason is treated as Infeasible (stuck)",
			conditions: []v1.PodCondition{{
				Type: v1.PodResizeInProgress, Status: v1.ConditionTrue, Reason: v1.PodReasonError,
			}},
			want: resizeStatusInfeasible,
		},
		{
			name: "Error in-progress outranks Deferred pending (Error first)",
			conditions: []v1.PodCondition{
				{Type: v1.PodResizeInProgress, Status: v1.ConditionTrue, Reason: v1.PodReasonError},
				{Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonDeferred},
			},
			want: resizeStatusInfeasible,
		},
		{
			name: "Error in-progress outranks Deferred pending (Deferred first)",
			conditions: []v1.PodCondition{
				{Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonDeferred},
				{Type: v1.PodResizeInProgress, Status: v1.ConditionTrue, Reason: v1.PodReasonError},
			},
			want: resizeStatusInfeasible,
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
// resizeTracker tests
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
// helpers
// ---------------------------------------------------------------------------

// fakeResizeTarget implements the resizeTarget interface for testing.
type fakeResizeTarget struct {
	selector  labels.Selector
	namespace string
	selfHeals func(ctx context.Context) bool
	patcher   func(resources map[string]v1.ResourceRequirements) error
	ownsPod   func(pod *v1.Pod) bool
	clientset clientset.Interface
	// templateAlreadyCurrent makes PatchTemplate report changed == false,
	// simulating a workload template that already matches desired.
	templateAlreadyCurrent bool
}

func (f *fakeResizeTarget) OwnsPod(pod *v1.Pod) bool {
	if f.ownsPod != nil {
		return f.ownsPod(pod)
	}
	return true // default: the target owns every selector-matched pod
}

func (f *fakeResizeTarget) GetOwnedPods(ctx context.Context) ([]v1.Pod, error) {
	if f.clientset == nil {
		return nil, nil
	}
	list, err := f.clientset.CoreV1().Pods(f.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: f.selector.String(),
	})
	if err != nil {
		return nil, err
	}
	var owned []v1.Pod
	for i := range list.Items {
		pod := &list.Items[i]
		if f.ownsPod != nil && !f.ownsPod(pod) {
			continue
		}
		owned = append(owned, *pod)
	}
	return owned, nil
}

func (f *fakeResizeTarget) GetPodSelector(ctx context.Context) (labels.Selector, error) {
	return f.selector, nil
}

func (f *fakeResizeTarget) IsSelfHealing(ctx context.Context) (bool, error) {
	if f.selfHeals != nil {
		return f.selfHeals(ctx), nil
	}
	return false, nil
}

func (f *fakeResizeTarget) PatchTemplate(ctx context.Context, resources map[string]v1.ResourceRequirements) (bool, error) {
	if f.patcher != nil {
		if err := f.patcher(resources); err != nil {
			return false, err
		}
	}
	// templateAlreadyCurrent simulates a template that already matches desired,
	// so the patch is a no-op (changed == false).
	return !f.templateAlreadyCurrent, nil
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
		clientset: client,
		patcher:   func(resources map[string]v1.ResourceRequirements) error { return nil },
	}
	r := &podResizer{
		resizeMode:     mode,
		fallbackConfig: fallback,
		dryRun:         dryRun,
		clock:          clock.RealClock{},
		clientset:      client,
		tracker:        tracker,
	}
	return r.resizeRunningPods(ctx, fake, desired)
}

// ---------------------------------------------------------------------------
// resize_runningpods_test.go: podResizer tests
// ---------------------------------------------------------------------------

// TestResizeRunningPods_SkipsNotOwnedPod verifies that a pod matching the
// selector but not owned by the target is neither resized nor disrupted.
func TestResizeRunningPods_SkipsNotOwnedPod(t *testing.T) {
	oldRes := cpuReq("100m")
	newRes := cpuReq("200m")

	foreign := makePod("foreign", v1.PodRunning, nil, oldRes) // would need a patch if considered

	patchCount, disruptCount := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{foreign}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			patchCount++
			w.Header().Set("Content-Type", "application/json")
			w.Write(podResponse(&foreign))
		case req.Method == "DELETE" || (req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/eviction")):
			disruptCount++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	rig := newResizeRig(t, server)
	fallback := ResizeFallbackConfig{GracePeriod: time.Millisecond, MaxPodsPerCycle: 1, DisruptionMethod: FallbackDisruptionDelete}
	fake := &fakeResizeTarget{
		selector:  rig.selector,
		namespace: "test",
		clientset: rig.client,
		selfHeals: func(ctx context.Context) bool { return false },
		patcher:   func(map[string]v1.ResourceRequirements) error { return nil },
		ownsPod:   func(pod *v1.Pod) bool { return false }, // target owns nothing
	}
	r := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		clock:          clock.RealClock{},
		clientset:      rig.client,
		tracker:        rig.tracker,
	}

	result, err := r.resizeRunningPods(context.Background(), fake, map[string]v1.ResourceRequirements{"main": newRes})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if patchCount != 0 || disruptCount != 0 {
		t.Errorf("foreign pod was touched: patches=%d disruptions=%d, want 0/0", patchCount, disruptCount)
	}
	if result.Applied != 0 || result.Evicted != 0 {
		t.Errorf("Applied=%d Evicted=%d, want 0/0", result.Applied, result.Evicted)
	}
}

// TestResizeRunningPods_RepatchResetsGraceClock guards against premature
// eviction when a pod that was already being tracked (e.g. stuck reaching a
// previous desired size) is re-patched to a new desired size after a cluster
// resize. The successful patch must reset the fallback clock so the new resize
// attempt gets a full grace period; the post-patch actuation mismatch (kubelet
// has not actuated yet) must NOT be measured against the stale timestamp.
func TestResizeRunningPods_RepatchResetsGraceClock(t *testing.T) {
	oldRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")}}
	newRes := v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m")}}

	// Pod spec is still at the old size (so needsPatch is true for newRes), and
	// the kubelet has not yet actuated (status still reports old size).
	runningOldStatus := []v1.ContainerStatus{{
		Name:      "main",
		State:     v1.ContainerState{Running: &v1.ContainerStateRunning{}},
		Resources: &oldRes,
	}}
	listed := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "pod-a", UID: types.UID("pod-a"), Labels: map[string]string{"app": "test"}},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "main", Resources: oldRes}}},
		Status:     v1.PodStatus{Phase: v1.PodRunning, ContainerStatuses: runningOldStatus},
	}
	// The object returned by the /resize patch: spec now at newRes, but status
	// still at oldRes (actuation pending) -> post-patch actuationMismatch.
	patched := listed
	patched.Spec = v1.PodSpec{Containers: []v1.Container{{Name: "main", Resources: newRes}}}

	deleteCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{listed}))
		case req.Method == "PATCH" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a/resize":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podResponse(&patched))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleteCount++
			w.WriteHeader(http.StatusOK)
		case req.Method == "POST" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a/eviction":
			deleteCount++ // count eviction as a disruption too
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1, DisruptionMethod: FallbackDisruptionDelete}
	rig := newResizeRig(t, server)
	// Stale entry from the previous (now superseded) desired size: well past grace.
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, false)
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
	if deleteCount != 0 {
		t.Errorf("pod was disrupted %d time(s); want 0 — the fresh patch should reset the grace clock", deleteCount)
	}
	if _, tracked := rig.tracker.notResizedSince[types.UID("pod-a")]; !tracked {
		t.Errorf("expected a fresh tracker entry started at patch time, found none")
	}
}

// TestResizeRunningPods_AllAlreadyOK verifies that when every pod already
// matches the desired resources we get AlreadyOK == pod count.
func TestResizeRunningPods_AllAlreadyOK(t *testing.T) {
	res := reqs(t, "100m", "128Mi")
	pods := []v1.Pod{
		makePod("pod-a", v1.PodRunning, nil, res),
		makePod("pod-b", v1.PodRunning, nil, res),
	}
	server := newPodListServer(pods)
	defer server.Close()
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
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
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, inProgressConds(), res)

	server := newPodListServer([]v1.Pod{pod})
	defer server.Close()
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0 (no patch needed)", result.Applied)
	}
	if result.InProgress != 1 {
		t.Errorf("InProgress = %d, want 1", result.InProgress)
	}
}

// TestResizeRunningPods_Deferred verifies Deferred counting via the no-patch path.
func TestResizeRunningPods_Deferred(t *testing.T) {
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, deferredConds(), res)

	server := newPodListServer([]v1.Pod{pod})
	defer server.Close()
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
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
	oldRes := cpuReq("50m")
	newRes := cpuReq("100m")

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
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
	rig := newResizeRig(t, server)
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}

	// Phase 1: patch sent; no conditions yet → Applied=1, no delete.
	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, false)
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}
	if deleted {
		t.Error("pod was deleted before grace period expired")
	}

	// Phase 2: kubelet wrote Infeasible; spec already matches desired.
	// Pre-seed tracker so grace has elapsed.
	pod.Spec.Containers = append([]v1.Container(nil), pod.Spec.Containers...)
	pod.Spec.Containers[0].Resources = newRes
	pod.Status.Conditions = infeasibleConds()
	rig.pastGrace("pod-a")

	deleted = false
	result = rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, fallback, false)
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
	res := cpuReq("50m")
	newRes := cpuReq("100m")

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
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
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
	oldRes := cpuReq("50m")
	newRes := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
	if result.Transient != 1 {
		t.Errorf("Transient = %d, want 1", result.Transient)
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
	res := cpuReq("100m")
	// Pods already have desired spec but are Infeasible so the no-patch
	// path classifies them and the fallback can fire.
	podA := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)
	podB := makePod("pod-b", v1.PodRunning, infeasibleConds(), res)

	deleteCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{podA, podB}))
		case req.Method == "DELETE" && strings.HasPrefix(req.URL.Path, "/api/v1/namespaces/test/pods/"):
			deleteCount++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a", "pod-b")

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, stdFallback, false)
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
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, stdFallback, false)
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
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodPending, inProgressConds(), res)

	server := newPodListServer([]v1.Pod{pod})
	defer server.Close()
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
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
	oldRes := cpuReq("50m")
	newRes := cpuReq("100m")

	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
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
	rig := newResizeRig(t, server)

	// First cycle: patch accepted, but no conditions yet → Applied=1.
	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
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
	pod.Status.Conditions = inProgressConds()

	result = rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
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
	oldRes := cpuReq("50m")
	newRes := cpuReq("100m")

	// pod-a and pod-c still need a resize and will accept it in place.
	podA := makePod("pod-a", v1.PodRunning, nil, oldRes)
	podC := makePod("pod-c", v1.PodRunning, nil, oldRes)
	// pod-b is already at the desired spec but stuck Infeasible from a prior
	// cycle — it is the "one of many" that fails to resize within the period.
	podB := makePod("pod-b", v1.PodRunning, infeasibleConds(), newRes)

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

	rig := newResizeRig(t, server)
	rig.tracker.notResizedSince[types.UID("pod-b")] = time.Now().Add(-10 * time.Minute) // past grace

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	patcher := func(resources map[string]v1.ResourceRequirements) error { templatePatches++; return nil }
	fake := &fakeResizeTarget{
		selector:  rig.selector,
		namespace: "test",
		clientset: rig.client,
		selfHeals: func(ctx context.Context) bool { return false },
		patcher:   patcher,
	}
	r := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		clock:          clock.RealClock{},
		clientset:      rig.client,
		tracker:        rig.tracker,
	}
	result, err := r.resizeRunningPods(context.Background(), fake, map[string]v1.ResourceRequirements{"main": newRes})
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

// TestResizeRunningPods_SelfHealingTemplateAlreadyCurrent verifies that when a
// self-healing target's template already matches desired (the rollout was
// triggered earlier or another pod did it) but a pod is still stuck past grace,
// cpvpa does NOT report a recreate or reset the clock — it counts RecreateStuck
// and keeps the tracker entry so the wedged state stays visible.
func TestResizeRunningPods_SelfHealingTemplateAlreadyCurrent(t *testing.T) {
	newRes := cpuReq("100m")
	podA := makePod("pod-a", v1.PodRunning, infeasibleConds(), newRes)

	deleted := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{podA}))
		case req.Method == "DELETE" || (req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/eviction")):
			deleted++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	rig := newResizeRig(t, server)
	// Stuck well past 3x the grace period, so the escalation branch is exercised.
	rig.tracker.notResizedSince[types.UID("pod-a")] = time.Now().Add(-20 * time.Minute)

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	fake := &fakeResizeTarget{
		selector:               rig.selector,
		namespace:              "test",
		clientset:              rig.client,
		selfHeals:              func(ctx context.Context) bool { return true },
		templateAlreadyCurrent: true, // PatchTemplate is a no-op -> changed == false
	}
	r := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		clock:          clock.RealClock{},
		clientset:      rig.client,
		tracker:        rig.tracker,
	}

	result, err := r.resizeRunningPods(context.Background(), fake, map[string]v1.ResourceRequirements{"main": newRes})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RecreateStuck != 1 {
		t.Errorf("RecreateStuck = %d, want 1", result.RecreateStuck)
	}
	if result.RecreateTriggered != 0 {
		t.Errorf("RecreateTriggered = %d, want 0 (no-op template patch must not count as a recreate)", result.RecreateTriggered)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (self-healing target must never be disrupted directly)", deleted)
	}
	if _, ok := rig.tracker.notResizedSince[types.UID("pod-a")]; !ok {
		t.Errorf("tracker entry was cleared; the wedged pod's stuck duration must be retained")
	}
}

// TestResizeRunningPods_FallbackSelfHealingNoDelete verifies that for a
// self-healing target (Deployment / RollingUpdate DaemonSet) the fallback
// patches the template and lets the controller recreate the pod, WITHOUT a
// manual delete.
func TestResizeRunningPods_FallbackSelfHealingNoDelete(t *testing.T) {
	res := cpuReq("100m")
	podA := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

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
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, stdFallback, true)
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
	res := cpuReq("100m")
	// Spec already at desired (no-patch branch) but stuck Deferred.
	pod := makePod("pod-a", v1.PodRunning, deferredConds(), res)

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

	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1}
	patcher := func(resources map[string]v1.ResourceRequirements) error { templatePatches++; return nil }
	fake := &fakeResizeTarget{
		selector:  rig.selector,
		namespace: "test",
		clientset: rig.client,
		selfHeals: func(ctx context.Context) bool { return false },
		patcher:   patcher,
	}
	resizer := &podResizer{
		resizeMode:     ResizeModeInPlaceOrRecreate,
		fallbackConfig: fallback,
		clock:          clock.RealClock{},
		clientset:      rig.client,
		tracker:        rig.tracker,
	}
	result, err := resizer.resizeRunningPods(context.Background(), fake, map[string]v1.ResourceRequirements{"main": res})
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
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, deferredConds(), res)

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
	rig := newResizeRig(t, server) // tracker not pre-seeded: first time seen, age ~0

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, stdFallback, false)
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
	if _, ok := rig.tracker.notResizedSince[types.UID("pod-a")]; !ok {
		t.Errorf("expected pod-a to be tracked as not resized")
	}
}

// TestResizeRunningPods_InvalidPatchNoPanic is a regression test: a synchronous
// Invalid (HTTP 422) rejection from the /resize patch must not panic the loop.
func TestResizeRunningPods_InvalidPatchNoPanic(t *testing.T) {
	oldRes := cpuReq("50m")
	newRes := cpuReq("100m")

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
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
	// pod-a's 422 is counted as Infeasible (not Errors); pod-b still resized.
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1 (pod-b processed after pod-a was rejected)", result.Applied)
	}
	if result.Infeasible != 1 {
		t.Errorf("Infeasible = %d, want 1 (pod-a Invalid → Infeasible)", result.Infeasible)
	}
	if result.Errors != 0 {
		t.Errorf("Errors = %d, want 0", result.Errors)
	}
}

// TestResizeRunningPods_Unexpected500 verifies that an unclassified error
// (e.g. 500 Internal Server Error) counts as Errors.
func TestResizeRunningPods_Unexpected500(t *testing.T) {
	oldRes := cpuReq("50m")
	newRes := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "PATCH" && strings.HasSuffix(req.URL.Path, "/resize"):
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	rig := newResizeRig(t, server)

	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlace, ResizeFallbackConfig{}, false)
	if result.Applied != 0 {
		t.Errorf("Applied = %d, want 0", result.Applied)
	}
	if result.Errors != 1 {
		t.Errorf("Errors = %d, want 1", result.Errors)
	}
	if result.Transient != 0 {
		t.Errorf("Transient = %d, want 0", result.Transient)
	}
}

// TestResizeRunningPods_DryRunNoFallbackDelete verifies that in dry-run mode
// an Infeasible pod past its grace period does not trigger any delete action.
func TestResizeRunningPods_DryRunNoFallbackDelete(t *testing.T) {
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.runDry(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, stdFallback, false)
	if result.Infeasible != 1 {
		t.Errorf("Infeasible = %d, want 1", result.Infeasible)
	}
	if deleted {
		t.Error("pod was deleted in dry-run mode")
	}
	if _, ok := rig.tracker.notResizedSince[types.UID("pod-a")]; !ok {
		t.Errorf("expected pod-a tracker entry to be retained in dry-run")
	}
}

// ---------------------------------------------------------------------------
// actuationState unit tests
// ---------------------------------------------------------------------------

func TestActuationState_Confirmed(t *testing.T) {
	res := reqs(t, "100m", "128Mi")
	pod := makePod("pod-a", v1.PodRunning, nil, res)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name:      "main",
		State:     v1.ContainerState{Running: &v1.ContainerStateRunning{}},
		Resources: &res,
	}}
	if got := actuationState(&pod, map[string]v1.ResourceRequirements{"main": res}); got != actuationConfirmed {
		t.Errorf("actuationState = %d, want actuationConfirmed", got)
	}
}

func TestActuationState_Mismatch(t *testing.T) {
	oldRes := reqs(t, "50m", "128Mi")
	newRes := reqs(t, "100m", "128Mi")
	pod := makePod("pod-a", v1.PodRunning, nil, oldRes)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name:      "main",
		State:     v1.ContainerState{Running: &v1.ContainerStateRunning{}},
		Resources: &oldRes,
	}}
	if got := actuationState(&pod, map[string]v1.ResourceRequirements{"main": newRes}); got != actuationMismatch {
		t.Errorf("actuationState = %d, want actuationMismatch", got)
	}
}

func TestActuationState_NilUnknown(t *testing.T) {
	res := reqs(t, "100m", "128Mi")
	pod := makePod("pod-a", v1.PodRunning, nil, res)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name:      "main",
		State:     v1.ContainerState{Running: &v1.ContainerStateRunning{}},
		Resources: nil,
	}}
	if got := actuationState(&pod, map[string]v1.ResourceRequirements{"main": res}); got != actuationUnknown {
		t.Errorf("actuationState = %d, want actuationUnknown", got)
	}
}

func TestActuationState_NotRunning(t *testing.T) {
	res := reqs(t, "100m", "128Mi")
	pod := makePod("pod-a", v1.PodRunning, nil, res)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name:      "main",
		State:     v1.ContainerState{Waiting: &v1.ContainerStateWaiting{}},
		Resources: nil,
	}}
	if got := actuationState(&pod, map[string]v1.ResourceRequirements{"main": res}); got != actuationConfirmed {
		t.Errorf("actuationState = %d, want actuationConfirmed (not-running container is ignored)", got)
	}
}

func TestActuationState_Unmanaged(t *testing.T) {
	res := reqs(t, "100m", "128Mi")
	pod := makePod("pod-a", v1.PodRunning, nil, res)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name:      "sidecar",
		State:     v1.ContainerState{Running: &v1.ContainerStateRunning{}},
		Resources: nil,
	}}
	if got := actuationState(&pod, map[string]v1.ResourceRequirements{"main": res}); got != actuationConfirmed {
		t.Errorf("actuationState = %d, want actuationConfirmed (unmanaged container is ignored)", got)
	}
}

// TestResizeRunningPods_ActuationMismatch triggers the fallback path when the
// kubelet reports stale resources (actuation mismatch) past the grace period.
func TestResizeRunningPods_ActuationMismatch(t *testing.T) {
	oldRes := cpuReq("50m")
	newRes := cpuReq("100m")

	pod := makePod("pod-a", v1.PodRunning, nil, newRes)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name:      "main",
		State:     v1.ContainerState{Running: &v1.ContainerStateRunning{}},
		Resources: &oldRes,
	}}

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": newRes}, ResizeModeInPlaceOrRecreate, stdFallback, false)
	if result.ActuationLag != 1 {
		t.Errorf("ActuationLag = %d, want 1", result.ActuationLag)
	}
	if result.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", result.Evicted)
	}
	if !deleted {
		t.Error("pod was NOT deleted after actuation mismatch past grace")
	}
}

// ---------------------------------------------------------------------------
// Eviction API unit tests (T2)
// ---------------------------------------------------------------------------

func TestResizeRunningPods_EvictionSuccess(t *testing.T) {
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

	evicted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "POST" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a/eviction":
			evicted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1, DisruptionMethod: FallbackDisruptionEviction}
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, fallback, false)
	if result.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", result.Evicted)
	}
	if !evicted {
		t.Error("pod was NOT evicted")
	}
	if _, ok := rig.tracker.notResizedSince[types.UID("pod-a")]; ok {
		t.Error("tracker entry should be cleared after successful eviction")
	}
}

func TestResizeRunningPods_EvictionBlockedByPDB(t *testing.T) {
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "POST" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a/eviction":
			w.WriteHeader(http.StatusTooManyRequests) // 429
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1, DisruptionMethod: FallbackDisruptionEviction}
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, fallback, false)
	if result.EvictionBlocked != 1 {
		t.Errorf("EvictionBlocked = %d, want 1", result.EvictionBlocked)
	}
	if result.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0 (blocked eviction must not count)", result.Evicted)
	}
	if result.Errors != 0 {
		t.Errorf("Errors = %d, want 0", result.Errors)
	}
	if _, ok := rig.tracker.notResizedSince[types.UID("pod-a")]; !ok {
		t.Error("tracker entry should be retained when eviction is blocked")
	}
	if result.RecreateTriggered != 0 {
		t.Errorf("RecreateTriggered = %d, want 0", result.RecreateTriggered)
	}
}

func TestResizeRunningPods_EvictionBlockedEscalation(t *testing.T) {
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "POST" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a/eviction":
			w.WriteHeader(http.StatusTooManyRequests) // 429
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1, DisruptionMethod: FallbackDisruptionEviction}
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")
	// Pre-seed eviction blocked time so it exceeds 3× grace (15 min).
	rig.tracker.evictionBlockedSince[types.UID("pod-a")] = time.Now().Add(-20 * time.Minute)

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, fallback, false)
	if result.EvictionBlocked != 1 {
		t.Errorf("EvictionBlocked = %d, want 1", result.EvictionBlocked)
	}
	// Escalation logs at Errorf level but does not increment Errors.
	if result.Errors != 0 {
		t.Errorf("Errors = %d, want 0", result.Errors)
	}
}

func TestResizeRunningPods_EvictionNotFound(t *testing.T) {
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "POST" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a/eviction":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1, DisruptionMethod: FallbackDisruptionEviction}
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, fallback, false)
	if result.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0", result.Evicted)
	}
	if result.Errors != 0 {
		t.Errorf("Errors = %d, want 0", result.Errors)
	}
	// Tracker should be cleared because the pod is already gone.
	if _, ok := rig.tracker.notResizedSince[types.UID("pod-a")]; ok {
		t.Error("tracker entry should be cleared when pod is NotFound")
	}
}

func TestResizeRunningPods_DeleteModeRegression(t *testing.T) {
	res := cpuReq("100m")
	pod := makePod("pod-a", v1.PodRunning, infeasibleConds(), res)

	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "GET" && req.URL.Path == "/api/v1/namespaces/test/pods":
			w.Header().Set("Content-Type", "application/json")
			w.Write(podListResponse([]v1.Pod{pod}))
		case req.Method == "DELETE" && req.URL.Path == "/api/v1/namespaces/test/pods/pod-a":
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	fallback := ResizeFallbackConfig{GracePeriod: 5 * time.Minute, MaxPodsPerCycle: 1, DisruptionMethod: FallbackDisruptionDelete}
	rig := newResizeRig(t, server)
	rig.pastGrace("pod-a")

	result := rig.run(map[string]v1.ResourceRequirements{"main": res}, ResizeModeInPlaceOrRecreate, fallback, false)
	if result.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", result.Evicted)
	}
	if !deleted {
		t.Error("pod was NOT deleted in delete mode")
	}
}
