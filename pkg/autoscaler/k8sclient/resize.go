/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package k8sclient — in-place resize extension.
//
// This file adds support for the /resize subresource (KEP-1287) on top of
// the existing PodTemplate-patching behaviour. The existing UpdateResources
// path is preserved and remains the default; in-place behaviour is opt-in
// via ResizeMode.
package k8sclient

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// ResizeMode controls how cpvpa applies resource changes to a target.
type ResizeMode string

const (
	// ResizeModeRecreate is the legacy behaviour: patch only the PodTemplate
	// and let the workload controller roll the pods. Always safe.
	ResizeModeRecreate ResizeMode = "Recreate"

	// ResizeModeInPlace patches the PodTemplate AND each live pod via the
	// /resize subresource. Pods whose resize is Deferred or Infeasible are
	// left as-is (they will be retried on the next poll cycle).
	ResizeModeInPlace ResizeMode = "InPlace"

	// ResizeModeInPlaceOrRecreate behaves like InPlace, but when a pod's
	// resize is reported as Infeasible for longer than the fallback grace
	// period, the pod is deleted so the owning controller can recreate it
	// (and the scheduler can place it elsewhere).
	ResizeModeInPlaceOrRecreate ResizeMode = "InPlaceOrRecreate"
)

// FallbackConfig governs the InPlaceOrRecreate fallback path.
type FallbackConfig struct {
	// GracePeriod is how long a pod must remain Infeasible before cpvpa
	// will delete it. Defaults to 5 minutes.
	GracePeriod time.Duration
	// MaxPodsPerCycle caps how many pods cpvpa will evict in a single poll
	// cycle, to avoid stampedes (especially on DaemonSets). Defaults to 1.
	MaxPodsPerCycle int
}

// ResizeResult is a per-cycle summary, useful for logging and metrics.
type ResizeResult struct {
	TargetPods int // pods owned by the target that we considered
	AlreadyOK  int // pods already at the desired resources
	Applied    int // /resize patches accepted by the API server
	InProgress int // kubelet has accepted and is applying
	Deferred   int // kubelet says: not now, maybe later (node pressure)
	Infeasible int // kubelet says: never on this node
	Evicted    int // pods deleted via the InPlaceOrRecreate fallback
	Errors     int // any other unexpected error per pod
}

// resizeTracker remembers the first time each pod was seen as Infeasible,
// so we can apply the fallback grace period. It is keyed by pod UID to
// survive pod-name reuse on DaemonSets.
type resizeTracker struct {
	mu             sync.Mutex
	infeasibleSeen map[types.UID]time.Time
}

func newResizeTracker() *resizeTracker {
	return &resizeTracker{infeasibleSeen: make(map[types.UID]time.Time)}
}

func (t *resizeTracker) markInfeasible(uid types.UID, now time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if first, ok := t.infeasibleSeen[uid]; ok {
		return first
	}
	t.infeasibleSeen[uid] = now
	return now
}

func (t *resizeTracker) clear(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.infeasibleSeen, uid)
}

// resizeRunningPods enumerates pods owned by the target controller and
// brings them to `desired` via the /resize subresource. It assumes the
// caller has already patched the PodTemplate, so new pods will come up at
// the desired size — this method only converges *existing* pods.
//
// The target selector is provided by the caller because the existing
// k8sClient already resolves the target (Deployment/ReplicaSet/DaemonSet)
// and knows its selector. We accept the selector as a parameter to keep
// this function decoupled from the target-kind switch.
func (k *k8sClient) resizeRunningPods(
	ctx context.Context,
	namespace string,
	selector labels.Selector,
	desired map[string]v1.ResourceRequirements,
	mode ResizeMode,
	fallback FallbackConfig,
	tracker *resizeTracker,
) (ResizeResult, error) {
	var result ResizeResult

	pods, err := k.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return result, fmt.Errorf("list pods: %w", err)
	}

	result.TargetPods = len(pods.Items)
	evictedThisCycle := 0
	now := time.Now()

	for i := range pods.Items {
		pod := &pods.Items[i]

		// Skip pods that are not actually running. New pods will come up
		// with the new template, and pods that are terminating are about
		// to disappear.
		if pod.Status.Phase != v1.PodRunning || pod.DeletionTimestamp != nil {
			continue
		}

		// Determine whether this pod actually needs a resize. We compare
		// against spec.containers (the desired state recorded on the pod),
		// not status — status reflects what's been applied, but spec is
		// what we're racing to converge.
		patchBody, needsPatch := buildResizePatch(pod, desired)
		if !needsPatch {
			result.AlreadyOK++
			tracker.clear(pod.UID)
			continue
		}

		// Issue the patch against the /resize subresource. Strategic merge
		// is the right choice here: it keys on container name and won't
		// clobber resizePolicy (JSON merge would replace the containers
		// array wholesale).
		_, patchErr := k.clientset.CoreV1().Pods(pod.Namespace).Patch(
			ctx,
			pod.Name,
			types.StrategicMergePatchType,
			patchBody,
			metav1.PatchOptions{FieldManager: "cpvpa"},
			"resize",
		)
		if patchErr != nil {
			// Infeasible / Deferred from the API server come back as
			// StatusUnprocessableEntity (422) in some kubelet versions;
			// more commonly they are reported asynchronously via pod
			// conditions, which we read below. So we don't fail hard here
			// unless it's clearly a different kind of error.
			if apierrors.IsInvalid(patchErr) {
				glog.V(2).Infof("resize rejected synchronously for pod=%s/%s: %v",
					pod.Namespace, pod.Name, patchErr)
			} else {
				glog.Errorf("resize patch error for pod=%s/%s: %v",
					pod.Namespace, pod.Name, patchErr)
				result.Errors++
				continue
			}
		} else {
			result.Applied++
		}

		// Re-fetch the pod to read the resize conditions the kubelet
		// produced. We do this in a tight loop because the conditions are
		// what tell us Deferred vs Infeasible vs InProgress; the patch
		// response itself doesn't carry that info.
		updated, getErr := k.clientset.CoreV1().Pods(pod.Namespace).
			Get(ctx, pod.Name, metav1.GetOptions{})
		if getErr != nil {
			glog.Warningf("failed to refetch pod=%s/%s after resize: %v",
				pod.Namespace, pod.Name, getErr)
			continue
		}

		status := classifyResize(updated)
		switch status {
		case resizeStatusInProgress:
			result.InProgress++
			tracker.clear(updated.UID)
		case resizeStatusDeferred:
			result.Deferred++
			tracker.clear(updated.UID) // not Infeasible — reset the timer
			glog.V(2).Infof("pod=%s/%s resize Deferred (node pressure)",
				updated.Namespace, updated.Name)
		case resizeStatusInfeasible:
			result.Infeasible++
			firstSeen := tracker.markInfeasible(updated.UID, now)
			age := now.Sub(firstSeen)
			glog.V(2).Infof("pod=%s/%s resize Infeasible (age=%s)",
				updated.Namespace, updated.Name, age)

			if mode == ResizeModeInPlaceOrRecreate &&
				age >= fallback.GracePeriod &&
				evictedThisCycle < fallback.MaxPodsPerCycle {

				if err := k.deleteForFallback(ctx, updated); err != nil {
					glog.Errorf("fallback delete failed for pod=%s/%s: %v",
						updated.Namespace, updated.Name, err)
					result.Errors++
				} else {
					glog.Infof("fallback-deleted pod=%s/%s (Infeasible for %s)",
						updated.Namespace, updated.Name, age)
					result.Evicted++
					evictedThisCycle++
					tracker.clear(updated.UID)
				}
			}
		case resizeStatusOK:
			// kubelet has caught up — nothing more to do.
			tracker.clear(updated.UID)
		}
	}

	return result, nil
}

// deleteForFallback uses a plain Delete with a short grace period. We
// intentionally do not call the eviction subresource in V1 — that would
// require PDB handling, 429/retry-after logic, etc. The user opts into
// this mode knowing pods may be deleted; MaxPodsPerCycle is their throttle.
func (k *k8sClient) deleteForFallback(ctx context.Context, pod *v1.Pod) error {
	grace := int64(30)
	return k.clientset.CoreV1().Pods(pod.Namespace).Delete(
		ctx, pod.Name,
		metav1.DeleteOptions{GracePeriodSeconds: &grace},
	)
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
		if resourcesEqual(c.Resources, want) {
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

func resourcesEqual(a, b v1.ResourceRequirements) bool {
	return resourceListEqual(a.Requests, b.Requests) &&
		resourceListEqual(a.Limits, b.Limits)
}

func resourceListEqual(a, b v1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !quantityEqual(av, bv) {
			return false
		}
	}
	return true
}

// quantityEqual compares two Quantities by value, not by string form —
// "1000m" and "1" should be treated as equal so we don't churn on
// every poll.
func quantityEqual(a, b resource.Quantity) bool {
	return a.Cmp(b) == 0
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
func classifyResize(pod *v1.Pod) resizeStatus {
	for _, c := range pod.Status.Conditions {
		switch c.Type {
		case v1.PodResizePending:
			if c.Reason == "Infeasible" {
				return resizeStatusInfeasible
			}
			return resizeStatusDeferred
		case v1.PodResizeInProgress:
			if c.Status == v1.ConditionTrue {
				return resizeStatusInProgress
			}
		}
	}
	return resizeStatusOK
}
