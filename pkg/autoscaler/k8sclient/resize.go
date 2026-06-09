/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package k8sclient

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ResizeMode controls how cpvpa applies resource changes to a target.
type ResizeMode string

const (
	// ResizeModeRecreate is the legacy behaviour: patch only the PodTemplate
	// and let the workload controller roll the pods. Always safe.
	ResizeModeRecreate ResizeMode = "Recreate"

	// ResizeModeInPlace resizes each live pod via the /resize subresource and
	// deliberately does NOT patch the PodTemplate (patching it would bump the
	// pod-template-hash and make the controller roll the pods). Newly created
	// pods start at the stale template size and converge on a later poll. Pods
	// whose resize is Deferred or Infeasible are retried on the next cycle.
	ResizeModeInPlace ResizeMode = "InPlace"

	// ResizeModeInPlaceOrRecreate behaves like InPlace, but when a pod's
	// resize is reported as Infeasible for longer than the fallback grace
	// period, the pod is deleted so the owning controller can recreate it
	// (and the scheduler can place it elsewhere).
	ResizeModeInPlaceOrRecreate ResizeMode = "InPlaceOrRecreate"
)

// ResizeFallbackConfig governs the InPlaceOrRecreate fallback path.
type ResizeFallbackConfig struct {
	// GracePeriod is how long a pod must remain Infeasible before cpvpa
	// will delete it. Defaults to 5 minutes.
	GracePeriod time.Duration
	// MaxPodsPerCycle caps how many pods cpvpa will evict in a single poll
	// cycle, to avoid stampedes (especially on DaemonSets). Defaults to 1.
	MaxPodsPerCycle int
}

// ResizeResult is a per-cycle summary, useful for logging and metrics.
type ResizeResult struct {
	TargetPods        int // pods owned by the target that we considered
	AlreadyOK         int // pods already at the desired resources
	Applied           int // /resize patches accepted by the API server
	InProgress        int // kubelet has accepted and is applying
	Deferred          int // kubelet says: not now, maybe later (node pressure)
	Infeasible        int // kubelet says: never on this node
	Evicted           int // pods cpvpa deleted directly (bare RS / OnDelete DS fallback)
	RecreateTriggered int // pods handed to the controller's rollout via a template patch
	Errors            int // any other unexpected error per pod
}

// resizeTracker remembers the first time each pod was continuously seen in a
// not-yet-completed resize state (Infeasible, Deferred, or InProgress), so we can apply
// the fallback grace period to any pod that fails to resize in time. It is
// keyed by pod UID to survive pod-name reuse on DaemonSets.
type resizeTracker struct {
	mu              sync.Mutex
	notResizedSince map[types.UID]time.Time
}

func newResizeTracker() *resizeTracker {
	return &resizeTracker{notResizedSince: make(map[types.UID]time.Time)}
}

// markNotResized records (once) when a pod first entered a not-yet-completed resize state and
// returns that timestamp. Repeat calls keep the original time so the grace
// period measures continuous, not most-recent, time-not-resized.
func (t *resizeTracker) markNotResized(uid types.UID, now time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if first, ok := t.notResizedSince[uid]; ok {
		return first
	}
	t.notResizedSince[uid] = now
	return now
}

func (t *resizeTracker) clear(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.notResizedSince, uid)
}

// retain drops tracker entries for UIDs not present in live, so pods that
// disappear while not resized (scaled down, drained, deleted elsewhere) don't
// leak entries across the lifetime of the process.
func (t *resizeTracker) retain(live map[types.UID]bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for uid := range t.notResizedSince {
		if !live[uid] {
			delete(t.notResizedSince, uid)
		}
	}
}

// --- patch building ---------------------------------------------------------

// buildResizePatch produces a strategic-merge patch body that brings the
// pod's containers to `desired`. It returns (nil, false) if nothing needs
// to change, which lets the caller skip the API call entirely (important
// because cpvpa polls every 10s and most cycles will be no-ops).
//
// The patch is scoped to containers cpvpa actually manages — containers in
// the pod that aren't in `desired` are left alone.
func buildResizePatch(pod *v1.Pod, desired map[string]v1.ResourceRequirements) ([]byte, bool) {
	type containerPatch struct {
		Name      string                  `json:"name"`
		Resources v1.ResourceRequirements `json:"resources"`
	}

	var changed []containerPatch
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		want, managed := desired[c.Name]
		if !managed {
			continue
		}
		if resourcesSatisfied(c.Resources, want) {
			continue
		}
		changed = append(changed, containerPatch{Name: c.Name, Resources: want})
	}
	if len(changed) == 0 {
		return nil, false
	}

	payload := map[string]any{
		"spec": map[string]any{
			"containers": changed,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		// Marshalling a fixed-shape struct can't realistically fail; if it
		// does, log and signal no-op so we don't crash the poll loop.
		glog.Errorf("failed to marshal resize patch: %v", err)
		return nil, false
	}
	return body, true
}

func resourcesSatisfied(a, b v1.ResourceRequirements) bool {
	return resourceListSatisfied(a.Requests, b.Requests) &&
		resourceListSatisfied(a.Limits, b.Limits)
}

func resourceListSatisfied(have, want v1.ResourceList) bool {
	for k, wv := range want {
		hv, ok := have[k]
		if !ok || hv.Cmp(wv) != 0 {
			return false
		}
	}
	return true
}

// --- status classification --------------------------------------------------

type resizeStatus int

const (
	resizeStatusOK resizeStatus = iota
	resizeStatusInProgress
	resizeStatusDeferred
	resizeStatusInfeasible
)

// classifyResize inspects the pod conditions added by the kubelet to
// figure out what state the resize is in.
//
// As of v1.33 beta the relevant condition types are:
//   - PodResizePending  (reason=Deferred | Infeasible)
//   - PodResizeInProgress
//
// Absence of both means: either the kubelet has caught up, or it hasn't
// observed the spec change yet. We treat absence as OK; if the kubelet
// is just slow, the next poll will reclassify.
//
// Precedence: PodResizePending (Infeasible > Deferred) wins over
// PodResizeInProgress. If PodResizeInProgress has Reason PodReasonError
// we treat it as an error state rather than InProgress so the tracker
// stays active and the pod may be fallback-deleted if infeasible.
func classifyResize(pod *v1.Pod) resizeStatus {
	var hasInProgress bool
	for _, c := range pod.Status.Conditions {
		switch c.Type {
		case v1.PodResizePending:
			if c.Reason == v1.PodReasonInfeasible {
				return resizeStatusInfeasible
			}
			return resizeStatusDeferred
		case v1.PodResizeInProgress:
			if c.Status == v1.ConditionTrue && c.Reason != v1.PodReasonError {
				hasInProgress = true
			}
		}
	}
	if hasInProgress {
		return resizeStatusInProgress
	}
	return resizeStatusOK
}
