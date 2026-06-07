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

	// Whether the target recreates pods by itself when its template changes.
	// Computed once per cycle and only when the fallback could fire.
	selfHeals := false
	if mode == ResizeModeInPlaceOrRecreate {
		selfHeals = k.targetSelfHeals()
	}

	// The template is patched at most once per cycle, lazily, and only when a
	// pod must actually be recreated (the InPlaceOrRecreate fallback). This is
	// what keeps a successful in-place resize from triggering a rollout, while
	// still ensuring a recreated pod comes back at the new size (no flapping).
	templatePatched := false
	ensureTemplate := func() error {
		if templatePatched {
			return nil
		}
		if err := k.patchTemplateForResize(desired); err != nil {
			return err
		}
		templatePatched = true
		if selfHeals {
			glog.Warningf("InPlaceOrRecreate fallback: patched %s/%s template; the controller will perform a workload-wide rolling update",
				k.target.Kind, k.target.Name)
		}
		return nil
	}

	// UIDs seen this cycle, used to prune stale tracker entries at the end.
	live := make(map[types.UID]bool, len(pods.Items))

	for i := range pods.Items {
		pod := &pods.Items[i]
		live[pod.UID] = true

		// A slow pod must not starve the rest. If the per-cycle deadline is
		// hit, stop cleanly; remaining pods (and any partial failure) are
		// reconsidered on the next poll — every step here is idempotent.
		if err := ctx.Err(); err != nil {
			glog.Warningf("resize cycle truncated after %d/%d pods: %v", i, len(pods.Items), err)
			break
		}

		// Skip terminal and terminating pods. Include Pending pods so
		// they get the correct resources before the kubelet admits them.
		if (pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending) || pod.DeletionTimestamp != nil {
			continue
		}

		// Determine whether this pod actually needs a resize. We compare
		// against spec.containers (the desired state recorded on the pod),
		// not status — status reflects what's been applied, but spec is
		// what we're racing to converge.
		patchBody, needsPatch := buildResizePatch(pod, desired)
		if !needsPatch {
			// Spec already matches desired, but the kubelet may still report a
			// not-yet-completed resize state from a previous cycle (Deferred under node
			// pressure, Infeasible, or stuck InProgress). Classify from the
			// list data so we don't reset the grace timer.
			if status := classifyResize(pod); status == resizeStatusOK {
				result.AlreadyOK++
				tracker.clear(pod.UID)
			} else {
				k.accountNotResized(ctx, pod, status, now, mode, fallback, selfHeals,
					ensureTemplate, &evictedThisCycle, &result, tracker)
			}
			continue
		}

		// Issue the patch against the /resize subresource. Strategic merge
		// is the right choice here: it keys on container name and won't
		// clobber resizePolicy (JSON merge would replace the containers
		// array wholesale).
		if k.dryRun {
			glog.V(2).Infof("dry-run: would patch /resize for pod=%s/%s: %s",
				pod.Namespace, pod.Name, string(patchBody))
			continue
		}
		updated, patchErr := k.clientset.CoreV1().Pods(pod.Namespace).Patch(
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
			// conditions, which we read below.
			if apierrors.IsInvalid(patchErr) {
				glog.V(2).Infof("resize rejected synchronously for pod=%s/%s: %v",
					pod.Namespace, pod.Name, patchErr)
			} else if apierrors.IsNotFound(patchErr) || apierrors.IsConflict(patchErr) {
				// Pod was deleted or modified concurrently — transient,
				// will be retried on the next poll.
				glog.V(2).Infof("resize patch transient error for pod=%s/%s: %v",
					pod.Namespace, pod.Name, patchErr)
				continue
			} else {
				glog.Errorf("resize patch error for pod=%s/%s: %v",
					pod.Namespace, pod.Name, patchErr)
				result.Errors++
				continue
			}
		} else {
			result.Applied++
		}

		// Classify the post-patch state. If still not OK, hand off to
		// accountNotResized for tracking and potential fallback.
		if status := classifyResize(updated); status == resizeStatusOK {
			// kubelet has caught up — nothing more to do.
			tracker.clear(updated.UID)
		} else {
			k.accountNotResized(ctx, updated, status, now, mode, fallback, selfHeals,
				ensureTemplate, &evictedThisCycle, &result, tracker)
		}
	}

	// Drop tracker entries for pods that no longer exist.
	tracker.retain(live)

	return result, nil
}

// accountNotResized records a not-yet-completed resize state for one pod (Infeasible,
// Deferred, or stuck InProgress): it bumps the matching state counter,
// advances the not-resized timer, and triggers the recreate fallback once the pod
// has been continuously not resized for the grace period. cpvpa treats every not-yet-completed
// state the same way — recreate after grace — for simplicity and a strong
// convergence guarantee; the Deferred and Infeasible counters are kept
// distinct only for observability (Deferred ~= cluster under pressure,
// Infeasible ~= request larger than any node). OK states are handled by the
// caller (a no-patch pod is AlreadyOK; a freshly patched pod was Applied).
func (k *k8sClient) accountNotResized(
	ctx context.Context,
	pod *v1.Pod,
	status resizeStatus,
	now time.Time,
	mode ResizeMode,
	fallback FallbackConfig,
	selfHeals bool,
	ensureTemplate func() error,
	evictedThisCycle *int,
	result *ResizeResult,
	tracker *resizeTracker,
) {
	switch status {
	case resizeStatusInProgress:
		result.InProgress++
	case resizeStatusDeferred:
		result.Deferred++
	case resizeStatusInfeasible:
		result.Infeasible++
	}
	firstSeen := tracker.markNotResized(pod.UID, now)
	age := now.Sub(firstSeen)
	glog.V(2).Infof("pod=%s/%s resize %s (not resized for %s)", pod.Namespace, pod.Name, status, age)
	k.maybeFallbackEvict(ctx, pod, age, mode, fallback, selfHeals,
		ensureTemplate, evictedThisCycle, result, tracker)
}

// maybeFallbackEvict handles one not-resized pod under InPlaceOrRecreate: once it
// has been not resized (Infeasible, Deferred, or stuck InProgress) continuously for
// at least the grace period, it makes the pod get recreated at the new size.
// The template is patched first (so the replacement is correctly sized rather
// than flapping). For self-healing targets (Deployment, RollingUpdate
// DaemonSet) the template patch alone triggers a controlled rollout, so we do
// NOT delete the pod ourselves — that would bypass maxUnavailable/PDB. For
// other targets we delete the pod so its controller recreates it, throttled by
// MaxPodsPerCycle.
//
// It operates per pod, so one pod failing to resize never blocks the others:
// the healthy pods in the same cycle are resized in place independently, and
// only the stuck pod(s) — up to the per-cycle cap — are recreated.
func (k *k8sClient) maybeFallbackEvict(
	ctx context.Context,
	pod *v1.Pod,
	age time.Duration,
	mode ResizeMode,
	fallback FallbackConfig,
	selfHeals bool,
	ensureTemplate func() error,
	evictedThisCycle *int,
	result *ResizeResult,
	tracker *resizeTracker,
) {
	if mode != ResizeModeInPlaceOrRecreate || age < fallback.GracePeriod {
		return
	}
	// Manual deletes are throttled per cycle; controller-driven rollouts pace
	// themselves, so the cap does not apply to self-healing targets.
	if !selfHeals && *evictedThisCycle >= fallback.MaxPodsPerCycle {
		return
	}
	// The replacement is created from the workload template, so it must carry
	// the new size before we trigger the recreate.
	if err := ensureTemplate(); err != nil {
		glog.Errorf("fallback: template patch failed, not recreating pod=%s/%s this cycle: %v",
			pod.Namespace, pod.Name, err)
		result.Errors++
		return
	}
	if selfHeals {
		// The template change triggers the controller's rolling recreate.
		glog.Infof("fallback: template updated; controller will recreate pod=%s/%s (not resized for %s)",
			pod.Namespace, pod.Name, age)
		result.RecreateTriggered++
		tracker.clear(pod.UID)
		return
	}
	if err := k.deleteForFallback(ctx, pod); err != nil {
		glog.Errorf("fallback delete failed for pod=%s/%s: %v", pod.Namespace, pod.Name, err)
		result.Errors++
		return
	}
	glog.Infof("fallback-deleted pod=%s/%s (not resized for %s)", pod.Namespace, pod.Name, age)
	result.Evicted++
	*evictedThisCycle++
	tracker.clear(pod.UID)
}

// deleteForFallback uses a plain Delete with a short grace period. We
// intentionally do not call the eviction subresource in V1 — that would
// require PDB handling, 429/retry-after logic, etc. The user opts into
// this mode knowing pods may be deleted; MaxPodsPerCycle is their throttle.
func (k *k8sClient) deleteForFallback(ctx context.Context, pod *v1.Pod) error {
	grace := int64(30)
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = *pod.Spec.TerminationGracePeriodSeconds
	}
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
