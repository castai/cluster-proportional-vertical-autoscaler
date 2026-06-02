/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package k8sclient

import (
	"encoding/json"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

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

// Tracker contract: first-seen timestamp is stable across repeat calls
// (so the grace period actually means something), and clear() resets it
// — important because a pod that goes Deferred → Infeasible → Deferred
// should not trigger a fallback delete based on its old Infeasible time.
func TestResizeTracker_FirstSeenIsStable(t *testing.T) {
	tr := newResizeTracker()
	uid := types.UID("abc")
	t0 := time.Now()
	first := tr.markInfeasible(uid, t0)
	if !first.Equal(t0) {
		t.Fatalf("first call should return now, got %v", first)
	}
	t1 := t0.Add(2 * time.Minute)
	second := tr.markInfeasible(uid, t1)
	if !second.Equal(t0) {
		t.Fatalf("second call should still return original timestamp, got %v", second)
	}
	tr.clear(uid)
	third := tr.markInfeasible(uid, t1)
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
	tr.markInfeasible(types.UID("pod-v1"), t0)
	tr.clear(types.UID("pod-v1")) // fallback deleted it
	got := tr.markInfeasible(types.UID("pod-v2"), t0.Add(time.Minute))
	if !got.Equal(t0.Add(time.Minute)) {
		t.Fatalf("new pod UID should get fresh timestamp")
	}
}

// prune should remove UIDs that are no longer in the pod list (e.g.
// pods deleted by node drain or scale-down), preventing unbounded growth.
func TestResizeTracker_Prune(t *testing.T) {
	tr := newResizeTracker()
	t0 := time.Now()
	tr.markInfeasible(types.UID("pod-a"), t0)
	tr.markInfeasible(types.UID("pod-b"), t0)

	// Only pod-a is still in the cluster.
	seen := map[types.UID]struct{}{
		types.UID("pod-a"): {},
	}
	tr.prune(seen)

	if _, ok := tr.infeasibleSeen[types.UID("pod-a")]; !ok {
		t.Fatal("pod-a should still be tracked")
	}
	if _, ok := tr.infeasibleSeen[types.UID("pod-b")]; ok {
		t.Fatal("pod-b should have been pruned")
	}
}

// Suppress unused-import warnings in case the test file is read alone.
var _ = metav1.ObjectMeta{}

// TestMetrics verifies that Metrics records cumulative counts correctly
// and that Snapshot returns stable values.
func TestMetrics(t *testing.T) {
	m := &Metrics{}
	m.Record(ResizeResult{Applied: 2, Deferred: 1, Infeasible: 3, Evicted: 1, Errors: 1})
	m.Record(ResizeResult{Applied: 1, Deferred: 2, Infeasible: 0, Evicted: 0, Errors: 0})

	s := m.Snapshot()
	if s.Applied != 3 {
		t.Errorf("Applied = %d, want 3", s.Applied)
	}
	if s.Deferred != 3 {
		t.Errorf("Deferred = %d, want 3", s.Deferred)
	}
	if s.Infeasible != 3 {
		t.Errorf("Infeasible = %d, want 3", s.Infeasible)
	}
	if s.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", s.Evicted)
	}
	if s.Errors != 1 {
		t.Errorf("Errors = %d, want 1", s.Errors)
	}
}
