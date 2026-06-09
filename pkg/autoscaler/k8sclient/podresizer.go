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
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
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
	// GracePeriod is how long a pod must remain not-resized in-place
	// before cpvpa will patch the template and delete it.
	GracePeriod time.Duration
	// MaxPodsPerCycle caps how many pods cpvpa will evict in a single poll
	// cycle, to avoid stampedes (especially on DaemonSets). Defaults to 1.
	MaxPodsPerCycle int
}

type resizeTarget interface {
	GetPodSelector(ctx context.Context) (labels.Selector, error)
	IsSelfHealing(ctx context.Context) bool
	PatchTemplate(ctx context.Context, resources map[string]v1.ResourceRequirements) error
	Namespace() string
}

// podResizer orchestrates in-place pod resizing and recreation fallback.
type podResizer struct {
	resizeMode     ResizeMode
	fallbackConfig ResizeFallbackConfig
	dryRun         bool

	clientset kubernetes.Interface
	target    resizeTarget
	tracker   *resizeTracker
}

type resizeResult struct {
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

// resizeRunningPods enumerates pods owned by the target controller and
// brings them to `desired` via the /resize subresource.
func (r *podResizer) resizeRunningPods(ctx context.Context, desired map[string]v1.ResourceRequirements) (resizeResult, error) {
	var result resizeResult

	selector, err := r.target.GetPodSelector(ctx)
	if err != nil {
		return result, fmt.Errorf("resolve selector: %w", err)
	}

	pods, err := r.clientset.CoreV1().Pods(r.target.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return result, fmt.Errorf("list pods: %w", err)
	}

	result.TargetPods = len(pods.Items)
	evictedThisCycle := 0
	now := time.Now()

	selfHealing := false
	if r.resizeMode == ResizeModeInPlaceOrRecreate {
		selfHealing = r.target.IsSelfHealing(ctx)
	}

	templatePatched := false
	ensureTemplate := func() error {
		if templatePatched {
			return nil
		}
		if err := r.target.PatchTemplate(ctx, desired); err != nil {
			return err
		}
		templatePatched = true
		return nil
	}

	live := make(map[types.UID]bool, len(pods.Items))
	for i := range pods.Items {
		live[pods.Items[i].UID] = true
	}

	for i := range pods.Items {
		pod := &pods.Items[i]

		if err := ctx.Err(); err != nil {
			glog.Warningf("resize cycle truncated after %d/%d pods: %v", i, len(pods.Items), err)
			break
		}

		if (pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending) || pod.DeletionTimestamp != nil {
			continue
		}

		patchBody, needsPatch := buildResizePatch(pod, desired)
		if !needsPatch {
			if status := classifyResize(pod); status == resizeStatusOK {
				result.AlreadyOK++
				r.tracker.clear(pod.UID)
			} else {
				r.accountNotResized(ctx, pod, status, now, selfHealing, ensureTemplate, &evictedThisCycle, &result)
			}
			continue
		}

		if r.dryRun {
			glog.V(2).Infof("dry-run: would patch /resize for pod=%s/%s: %s",
				pod.Namespace, pod.Name, string(patchBody))
			continue
		}
		updated, patchErr := r.clientset.CoreV1().Pods(pod.Namespace).Patch(
			ctx,
			pod.Name,
			types.StrategicMergePatchType,
			patchBody,
			metav1.PatchOptions{},
			"resize",
		)
		if patchErr != nil {
			result.Errors++
			if apierrors.IsNotFound(patchErr) || apierrors.IsConflict(patchErr) {
				glog.V(2).Infof("resize patch transient error for pod=%s/%s: %v",
					pod.Namespace, pod.Name, patchErr)
				continue
			}
			if apierrors.IsInvalid(patchErr) {
				glog.V(2).Infof("resize rejected as invalid for pod=%s/%s (e.g. QoS class change); routing to fallback: %v",
					pod.Namespace, pod.Name, patchErr)
				r.accountNotResized(ctx, pod, resizeStatusInfeasible, now, selfHealing,
					ensureTemplate, &evictedThisCycle, &result)
				continue
			}
			glog.Errorf("resize patch error for pod=%s/%s: %v",
				pod.Namespace, pod.Name, patchErr)
			continue
		} else {
			result.Applied++
		}

		if status := classifyResize(updated); status == resizeStatusOK {
			r.tracker.clear(updated.UID)
		} else {
			r.accountNotResized(ctx, updated, status, now, selfHealing,
				ensureTemplate, &evictedThisCycle, &result)
		}
	}

	r.tracker.retain(live)
	return result, nil
}

// accountNotResized records a not-yet-completed resize state for one pod.
func (r *podResizer) accountNotResized(
	ctx context.Context,
	pod *v1.Pod,
	status resizeStatus,
	now time.Time,
	selfHeals bool,
	ensureTemplate func() error,
	evictedThisCycle *int,
	result *resizeResult,
) {
	switch status {
	case resizeStatusInProgress:
		result.InProgress++
	case resizeStatusDeferred:
		result.Deferred++
	case resizeStatusInfeasible:
		result.Infeasible++
	}
	firstSeen := r.tracker.markNotResized(pod.UID, now)
	age := now.Sub(firstSeen)
	glog.V(2).Infof("pod=%s/%s resize %s (not resized for %s)", pod.Namespace, pod.Name, status, age)
	r.maybeFallbackEvict(ctx, pod, age, selfHeals, ensureTemplate, evictedThisCycle, result)
}

// maybeFallbackEvict handles the InPlaceOrRecreate fallback for one not-resized pod.
func (r *podResizer) maybeFallbackEvict(
	ctx context.Context,
	pod *v1.Pod,
	age time.Duration,
	selfHeals bool,
	ensureTemplate func() error,
	evictedThisCycle *int,
	result *resizeResult,
) {
	if r.resizeMode != ResizeModeInPlaceOrRecreate || age < r.fallbackConfig.GracePeriod {
		return
	}
	if !selfHeals && *evictedThisCycle >= r.fallbackConfig.MaxPodsPerCycle {
		return
	}
	if err := ensureTemplate(); err != nil {
		glog.Errorf("fallback: template patch failed, not recreating pod=%s/%s this cycle: %v",
			pod.Namespace, pod.Name, err)
		result.Errors++
		return
	}
	if selfHeals {
		glog.Infof("fallback: template updated; controller will recreate pod=%s/%s (not resized for %s)",
			pod.Namespace, pod.Name, age)
		result.RecreateTriggered++
		r.tracker.clear(pod.UID)
		return
	}
	if err := r.deleteForFallback(ctx, pod); err != nil {
		glog.Errorf("fallback delete failed for pod=%s/%s: %v", pod.Namespace, pod.Name, err)
		result.Errors++
		return
	}
	glog.Infof("fallback-deleted pod=%s/%s (not resized for %s)", pod.Namespace, pod.Name, age)
	result.Evicted++
	*evictedThisCycle++
	r.tracker.clear(pod.UID)
}

// deleteForFallback deletes a pod so its controller recreates it at the new size.
func (r *podResizer) deleteForFallback(ctx context.Context, pod *v1.Pod) error {
	grace := int64(30)
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = *pod.Spec.TerminationGracePeriodSeconds
	}
	return r.clientset.CoreV1().Pods(pod.Namespace).Delete(
		ctx, pod.Name,
		metav1.DeleteOptions{GracePeriodSeconds: &grace},
	)
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

// EnsureResizeSubresource checks that the cluster supports the pods/resize
// subresource (Kubernetes 1.33+). Call at startup when resize mode is not
// Recreate to fail fast with a clear message.
//
// Note: this checks discovery (the API server advertises the subresource),
// but it cannot confirm the InPlacePodVerticalScaling feature gate is
// enabled on every kubelet. As of Kubernetes 1.33 the gate is on by
// default, so discovery presence is a sufficient signal.
func EnsureResizeSubresource(client kubernetes.Interface) error {
	resources, err := client.Discovery().ServerResourcesForGroupVersion("v1")
	if err != nil {
		return fmt.Errorf("failed to discover v1 API resources: %w", err)
	}
	for _, r := range resources.APIResources {
		if r.Name == "pods/resize" {
			return nil
		}
	}
	return fmt.Errorf("cluster does not support pods/resize subresource; " +
		"in-place pod resize requires Kubernetes 1.33+ with InPlacePodVerticalScaling enabled")
}
