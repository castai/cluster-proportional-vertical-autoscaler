/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8sclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
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

// FallbackDisruptionMethod controls how cpvpa recreates stuck pods.
type FallbackDisruptionMethod string

const (
	FallbackDisruptionEviction FallbackDisruptionMethod = "eviction"
	FallbackDisruptionDelete   FallbackDisruptionMethod = "delete"
)

// resizeRejection is a permanent reason why a /resize patch can never
// succeed for a given pod. Rejected pods are skipped (instead of being
// PATCHed again every poll) but still feed the InPlaceOrRecreate fallback,
// which recreates them without depending on in-place support.
type resizeRejection string

const (
	rejectNodeUnsupported resizeRejection = "node-without-in-place-support" // kubelet lacks InPlacePodVerticalScaling
	rejectQoSChange       resizeRejection = "qos-class-change"              // resize would change the pod's QoS class
	rejectAboveLimit      resizeRejection = "request-above-limit"           // a request would exceed its limit
	rejectPodLevel        resizeRejection = "pod-level-resources"           // pod uses pod-level resources (out of scope)
	rejectStaticPod       resizeRejection = "static-pod"
	rejectWindows         resizeRejection = "windows-pod"
)

// ResizeFallbackConfig governs the InPlaceOrRecreate fallback path.
type ResizeFallbackConfig struct {
	// GracePeriod is how long a pod must remain not-resized in-place
	// before cpvpa will patch the template and delete it.
	GracePeriod time.Duration
	// MaxPodsPerCycle caps how many pods cpvpa will evict in a single poll
	// cycle, to avoid stampedes (especially on DaemonSets). Defaults to 1.
	MaxPodsPerCycle int
	// DisruptionMethod controls how cpvpa recreates stuck pods.
	DisruptionMethod FallbackDisruptionMethod
}

type resizeTarget interface {
	IsSelfHealing(ctx context.Context) (bool, error)
	PatchTemplate(ctx context.Context, resources map[string]v1.ResourceRequirements) (changed bool, err error)
	GetOwnedPods(ctx context.Context) ([]v1.Pod, error)
}

// podResizer orchestrates in-place pod resizing and recreation fallback.
type podResizer struct {
	resizeMode     ResizeMode
	fallbackConfig ResizeFallbackConfig
	dryRun         bool
	clock          clock.PassiveClock
	clientset      kubernetes.Interface
	tracker        *resizeTracker
}

type resizeResult struct {
	TargetPods        int // pods owned by the target that we considered
	AlreadyOK         int // pods already at the desired resources
	Applied           int // /resize patches accepted by the API server
	InProgress        int // kubelet has accepted and is applying
	Deferred          int // kubelet says: not now, maybe later (node pressure)
	Infeasible        int // kubelet says: never on this node
	ActuationLag      int // kubelet reported resources but they differ from desired
	Evicted           int // pods cpvpa evicted or deleted (fallback)
	RecreateTriggered int // pods handed to the controller's rollout via a template patch
	RecreateStuck     int // self-healing pods still not resized though the template already matches desired (rollout wedged, likely unschedulable)
	EvictionBlocked   int // pods whose eviction was blocked by PDB
	Rejected          int // pods whose resize was skipped: permanently rejected (see tracker rejection reasons)
	Unsupported       int // subset of Rejected: pods on nodes whose kubelet lacks InPlacePodVerticalScaling
	ActuationUnknown  int // running containers whose kubelet has not reported status resources (implies Unsupported)
	WaitingToStart    int // scheduled pods skipped until at least one container is running
	Transient         int // transient errors (404 NotFound, 409 Conflict) per pod
	Errors            int // any other unexpected error per pod
}

// resizeRunningPods enumerates pods owned by the target controller and
// brings them to `desired` via the /resize subresource.
func (r *podResizer) resizeRunningPods(ctx context.Context, target resizeTarget, desired map[string]v1.ResourceRequirements) (resizeResult, error) {
	var result resizeResult

	pods, err := target.GetOwnedPods(ctx)
	if err != nil {
		return result, fmt.Errorf("get owned pods: %w", err)
	}

	result.TargetPods = len(pods)
	evictedThisCycle := 0
	now := r.clock.Now()

	selfHealing := false
	if r.resizeMode == ResizeModeInPlaceOrRecreate {
		selfHealing, err = target.IsSelfHealing(ctx)
		if err != nil {
			return result, fmt.Errorf("self-heal check: %w", err)
		}
	}

	templatePatched := false
	ensureTemplate := func() (bool, error) {
		if templatePatched {
			return true, nil // already changed earlier this cycle
		}
		changed, err := target.PatchTemplate(ctx, desired)
		if err != nil {
			return false, err
		}
		if changed {
			templatePatched = true
		}
		return changed, nil
	}

	live := make(map[types.UID]bool, len(pods))
	for i := range pods {
		live[pods[i].UID] = true
	}
	defer r.tracker.retain(live)

	for i := range pods {
		pod := &pods[i]

		if err := ctx.Err(); err != nil {
			glog.Warningf("resize cycle truncated after %d/%d pods: %v", i, len(pods), err)
			break
		}

		if (pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending) || pod.DeletionTimestamp != nil {
			continue
		}

		// The API server's node-support check needs at least one running
		// container; a scheduled pod with nothing running is picked up on a
		// later poll instead of wasting a PATCH. Unscheduled pods are fine:
		// the scheduler uses the new values.
		if pod.Spec.NodeName != "" && !hasRunningContainer(pod) {
			result.WaitingToStart++
			continue
		}

		// A pod whose resize is permanently rejected (determined this poll or
		// on an earlier one) is skipped; the fallback machinery still sees it
		// so InPlaceOrRecreate can converge it via recreation, which does not
		// depend on in-place support.
		if reason, rejected := r.tracker.rejection(pod.UID); rejected {
			r.noteRejection(ctx, pod, reason, now, selfHealing, ensureTemplate, &evictedThisCycle, &result)
			continue
		}
		if reason, rejected := precheckResize(pod, desired); rejected {
			r.tracker.setRejection(pod.UID, reason)
			r.noteRejection(ctx, pod, reason, now, selfHealing, ensureTemplate, &evictedThisCycle, &result)
			continue
		}

		patchBody, needsPatch := buildResizePatch(pod, desired)
		if !needsPatch {
			if status := classifyResize(pod); status == resizeStatusOK {
				switch actuationState(pod, desired) {
				case actuationConfirmed:
					result.AlreadyOK++
					r.tracker.clear(pod.UID)
				case actuationMismatch:
					result.ActuationLag++
					firstSeen := r.tracker.markNotResized(pod.UID, now)
					age := now.Sub(firstSeen)
					glog.V(2).Infof("pod=%s/%s actuation mismatch (not resized for %s)", pod.Namespace, pod.Name, age)
					r.maybeFallbackEvict(ctx, pod, now, age, selfHealing, ensureTemplate, &evictedThisCycle, &result)
				case actuationUnknown:
					// A running container with nil status.resources means the
					// kubelet reports no resize support: treat it as a permanent
					// node-unsupported rejection.
					result.ActuationUnknown++
					r.tracker.setRejection(pod.UID, rejectNodeUnsupported)
					r.noteRejection(ctx, pod, rejectNodeUnsupported, now, selfHealing, ensureTemplate, &evictedThisCycle, &result)
				}
			} else {
				r.accountNotResized(ctx, pod, status, now, selfHealing, ensureTemplate, &evictedThisCycle, &result)
			}
			continue
		}

		if r.dryRun {
			glog.Infof("dry-run: would patch /resize for pod=%s/%s: %s",
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
			if apierrors.IsNotFound(patchErr) || apierrors.IsConflict(patchErr) {
				result.Transient++
				glog.V(2).Infof("resize patch transient error for pod=%s/%s: %v",
					pod.Namespace, pod.Name, patchErr)
				continue
			}
			if apierrors.IsInvalid(patchErr) {
				if reason := rejectionReasonFromError(patchErr); reason != "" {
					r.tracker.setRejection(pod.UID, reason)
					r.noteRejection(ctx, pod, reason, now, selfHealing, ensureTemplate, &evictedThisCycle, &result)
				} else {
					glog.V(2).Infof("resize rejected as invalid for pod=%s/%s (e.g. QoS class change); routing to fallback: %v",
						pod.Namespace, pod.Name, patchErr)
					r.accountNotResized(ctx, pod, resizeStatusInfeasible, now, selfHealing,
						ensureTemplate, &evictedThisCycle, &result)
				}
				continue
			}
			result.Errors++
			glog.Errorf("resize patch error for pod=%s/%s: %v",
				pod.Namespace, pod.Name, patchErr)
			continue
		} else {
			result.Applied++
			// A successful patch is a fresh resize attempt, so reset the
			// fallback clock.
			r.tracker.clear(updated.UID)
		}

		if status := classifyResize(updated); status == resizeStatusOK {
			switch actuationState(updated, desired) {
			case actuationConfirmed:
				r.tracker.clear(updated.UID)
			case actuationMismatch:
				result.ActuationLag++
				firstSeen := r.tracker.markNotResized(updated.UID, now)
				age := now.Sub(firstSeen)
				glog.V(2).Infof("pod=%s/%s actuation mismatch (not resized for %s)", updated.Namespace, updated.Name, age)
				r.maybeFallbackEvict(ctx, updated, now, age, selfHealing, ensureTemplate, &evictedThisCycle, &result)
			case actuationUnknown:
				result.ActuationUnknown++
				r.tracker.setRejection(updated.UID, rejectNodeUnsupported)
				r.noteRejection(ctx, updated, rejectNodeUnsupported, now, selfHealing, ensureTemplate, &evictedThisCycle, &result)
			}
		} else {
			r.accountNotResized(ctx, updated, status, now, selfHealing,
				ensureTemplate, &evictedThisCycle, &result)
		}
	}

	return result, nil
}

// accountNotResized records a not-yet-completed resize state for one pod.
func (r *podResizer) accountNotResized(
	ctx context.Context,
	pod *v1.Pod,
	status resizeStatus,
	now time.Time,
	selfHeals bool,
	ensureTemplate func() (bool, error),
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
	r.maybeFallbackEvict(ctx, pod, now, age, selfHeals, ensureTemplate, evictedThisCycle, result)
}

// noteRejection accounts for one pod whose resize is permanently rejected:
// it counts the rejection, warns once per (pod, reason), and feeds the pod
// to the fallback machinery like an Infeasible state.
func (r *podResizer) noteRejection(
	ctx context.Context,
	pod *v1.Pod,
	reason resizeRejection,
	now time.Time,
	selfHeals bool,
	ensureTemplate func() (bool, error),
	evictedThisCycle *int,
	result *resizeResult,
) {
	result.Rejected++
	if reason == rejectNodeUnsupported {
		result.Unsupported++
	}
	if r.tracker.setRejection(pod.UID, reason) {
		if reason == rejectNodeUnsupported {
			glog.Warningf("pod=%s/%s on node %q cannot resize in place: kubelet has no InPlacePodVerticalScaling support",
				pod.Namespace, pod.Name, pod.Spec.NodeName)
		} else {
			glog.Warningf("pod=%s/%s resize permanently rejected (%s); skipping /resize patches for it",
				pod.Namespace, pod.Name, reason)
		}
	}
	r.accountNotResized(ctx, pod, resizeStatusInfeasible, now, selfHeals, ensureTemplate, evictedThisCycle, result)
}

// hasRunningContainer reports whether at least one of the pod's containers
// is running.
func hasRunningContainer(pod *v1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].State.Running != nil {
			return true
		}
	}
	return false
}

// resizeSupportedOnNode mirrors the API server's isPodResizeRequestSupported:
// the kubelet advertises InPlacePodVerticalScaling by reporting
// status.containerStatuses[].resources on running containers. Callers must
// ensure at least one container is running first.
func resizeSupportedOnNode(pod *v1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Running != nil {
			return cs.Resources != nil
		}
	}
	return true
}

// precheckResize replicates the API server's permanent /resize rejections
// (ValidatePodResize) locally, so a doomed PATCH is not sent every poll.
// Anything it rejects that we fail to predict is still handled by the
// Invalid-response path.
func precheckResize(pod *v1.Pod, desired map[string]v1.ResourceRequirements) (resizeRejection, bool) {
	if pod.Spec.Resources != nil {
		return rejectPodLevel, true
	}
	if _, isMirror := pod.Annotations[v1.MirrorPodAnnotationKey]; isMirror {
		return rejectStaticPod, true
	}
	if pod.Spec.OS != nil && pod.Spec.OS.Name == v1.Windows {
		return rejectWindows, true
	}
	if !resizeSupportedOnNode(pod) {
		return rejectNodeUnsupported, true
	}
	if requestAboveLimit(pod, desired) {
		return rejectAboveLimit, true
	}
	if podQoS(pod.Spec.Containers) != podQoS(mergedContainers(pod, desired)) {
		return rejectQoSChange, true
	}
	return "", false
}

// mergedContainers returns the pod's containers with `desired` resource keys
// overridden, i.e. what the pod spec would look like after the resize. Only
// containers and resource keys cpvpa manages change.
func mergedContainers(pod *v1.Pod, desired map[string]v1.ResourceRequirements) []v1.Container {
	out := make([]v1.Container, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		c := pod.Spec.Containers[i]
		if want, managed := desired[c.Name]; managed {
			c.Resources.Requests = mergeResourceList(c.Resources.Requests, want.Requests)
			c.Resources.Limits = mergeResourceList(c.Resources.Limits, want.Limits)
		}
		out[i] = c
	}
	return out
}

func mergeResourceList(have, want v1.ResourceList) v1.ResourceList {
	if len(want) == 0 {
		return have
	}
	out := make(v1.ResourceList, len(have)+len(want))
	for k, v := range have {
		out[k] = v
	}
	for k, v := range want {
		out[k] = v
	}
	return out
}

// requestAboveLimit reports whether, after merging `desired` over the pod's
// current resources, any managed container request would exceed its limit.
func requestAboveLimit(pod *v1.Pod, desired map[string]v1.ResourceRequirements) bool {
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		want, managed := desired[c.Name]
		if !managed {
			continue
		}
		reqs := mergeResourceList(c.Resources.Requests, want.Requests)
		lims := mergeResourceList(c.Resources.Limits, want.Limits)
		for name, r := range reqs {
			if l, ok := lims[name]; ok && r.Cmp(l) > 0 {
				return true
			}
		}
	}
	return false
}

// podQoS computes the pod's QoS class (cpu and memory, containers only —
// init containers are identical on both sides of the comparison) following
// the upstream algorithm: Guaranteed when every container sets cpu+memory
// requests equal to limits, BestEffort when nothing is set, Burstable
// otherwise.
func podQoS(containers []v1.Container) v1.PodQOSClass {
	requests, limits := 0, 0
	isGuaranteed := true
	for i := range containers {
		c := &containers[i]
		for _, name := range []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory} {
			req, reqOK := c.Resources.Requests[name]
			lim, limOK := c.Resources.Limits[name]
			if reqOK {
				requests++
			}
			if limOK {
				limits++
			}
			if !reqOK || !limOK || req.Cmp(lim) != 0 {
				isGuaranteed = false
			}
		}
	}
	if requests == 0 && limits == 0 {
		return v1.PodQOSBestEffort
	}
	if isGuaranteed {
		return v1.PodQOSGuaranteed
	}
	return v1.PodQOSBurstable
}

// rejectionReasonFromError maps an API Invalid (422) response onto a known
// permanent rejection reason, mirroring ValidatePodResize's messages.
// Unknown causes return "" so the caller falls back to generic handling.
func rejectionReasonFromError(err error) resizeRejection {
	statusErr, ok := err.(apierrors.APIStatus)
	if !ok {
		return ""
	}
	st := statusErr.Status()
	msgs := make([]string, 0, 1+len(st.Details.Causes))
	if st.Message != "" {
		msgs = append(msgs, st.Message)
	}
	for _, c := range st.Details.Causes {
		if c.Message != "" {
			msgs = append(msgs, c.Message)
		}
	}
	for _, m := range msgs {
		switch {
		case strings.Contains(m, "without support for resize"):
			return rejectNodeUnsupported
		case strings.Contains(m, "QOS Class may not change"):
			return rejectQoSChange
		case strings.Contains(m, "less than or equal to"):
			return rejectAboveLimit
		}
	}
	return ""
}

// maybeFallbackEvict handles the InPlaceOrRecreate fallback for one not-resized pod.
func (r *podResizer) maybeFallbackEvict(
	ctx context.Context,
	pod *v1.Pod,
	now time.Time,
	age time.Duration,
	selfHeals bool,
	ensureTemplate func() (bool, error),
	evictedThisCycle *int,
	result *resizeResult,
) {
	if r.resizeMode != ResizeModeInPlaceOrRecreate || age < r.fallbackConfig.GracePeriod {
		return
	}
	if r.dryRun {
		glog.Infof("dry-run: would fall back (recreate/delete) pod=%s/%s (not resized for %s)",
			pod.Namespace, pod.Name, age)
		return
	}
	if !selfHeals && *evictedThisCycle >= r.fallbackConfig.MaxPodsPerCycle {
		return
	}
	changed, err := ensureTemplate()
	if err != nil {
		glog.Errorf("fallback: template patch failed, not recreating pod=%s/%s this cycle: %v",
			pod.Namespace, pod.Name, err)
		result.Errors++
		return
	}
	if selfHeals {
		if changed {
			glog.Infof("fallback: template updated; controller will recreate pod=%s/%s (not resized for %s)",
				pod.Namespace, pod.Name, age)
			result.RecreateTriggered++
			r.tracker.clear(pod.UID)
			return
		}
		// The template already matches desired (a prior cycle, or another pod,
		// triggered the rollout) yet this pod is still stuck past the grace
		// period. Re-patching is a no-op and would not start a new rollout, so
		// do NOT report a recreate or reset the clock — that would mask a
		// permanently wedged pod. The rollout is most likely blocked because the
		// desired size is unschedulable; surface it instead.
		result.RecreateStuck++
		if age > 3*r.fallbackConfig.GracePeriod {
			glog.Errorf("pod=%s/%s still not resized %s after its template was already brought to the desired size; "+
				"the rollout is likely wedged (desired size may be unschedulable) — manual intervention may be needed",
				pod.Namespace, pod.Name, age)
		} else {
			glog.V(2).Infof("pod=%s/%s still not resized %s; template already at desired, awaiting controller rollout",
				pod.Namespace, pod.Name, age)
		}
		return
	}

	// Perform the disruption: eviction or direct delete.
	if r.fallbackConfig.DisruptionMethod == FallbackDisruptionEviction {
		err = r.evictForFallback(ctx, pod)
	} else {
		err = r.deleteForFallback(ctx, pod)
	}
	if err != nil {
		if apierrors.IsTooManyRequests(err) {
			// PDB blocked the eviction. Keep the tracker entry, do NOT consume
			// MaxPodsPerCycle budget, and record blocked count.
			result.EvictionBlocked++
			blockedNow := now
			firstBlocked := r.tracker.markEvictionBlocked(pod.UID, blockedNow)
			blockedAge := blockedNow.Sub(firstBlocked)
			if blockedAge > 3*r.fallbackConfig.GracePeriod {
				glog.Errorf("eviction blocked for pod=%s/%s by PDB for longer than %s; manual intervention may be needed",
					pod.Namespace, pod.Name, blockedAge)
			} else {
				glog.V(2).Infof("eviction blocked for pod=%s/%s by PDB (blocked for %s, will retry)",
					pod.Namespace, pod.Name, blockedAge)
			}
			return
		}
		if apierrors.IsNotFound(err) {
			r.tracker.clear(pod.UID)
			return
		}
		glog.Errorf("fallback %s failed for pod=%s/%s: %v",
			string(r.fallbackConfig.DisruptionMethod), pod.Namespace, pod.Name, err)
		result.Errors++
		return
	}
	glog.Infof("fallback-%s pod=%s/%s (not resized for %s)",
		string(r.fallbackConfig.DisruptionMethod), pod.Namespace, pod.Name, age)
	result.Evicted++
	*evictedThisCycle++
	r.tracker.clear(pod.UID)
}

// evictForFallback evicts a pod via the Eviction API so the controller
// recreates it at the new size. The eviction honours PodDisruptionBudgets.
func (r *podResizer) evictForFallback(ctx context.Context, pod *v1.Pod) error {
	if r.dryRun {
		return nil
	}
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
	}
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		eviction.DeleteOptions = &metav1.DeleteOptions{
			GracePeriodSeconds: pod.Spec.TerminationGracePeriodSeconds,
		}
	}
	return r.clientset.CoreV1().Pods(pod.Namespace).EvictV1(ctx, eviction)
}

// deleteForFallback deletes a pod so its controller recreates it at the new size.
func (r *podResizer) deleteForFallback(ctx context.Context, pod *v1.Pod) error {
	if r.dryRun {
		return nil
	}
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
	mu                   sync.Mutex
	notResizedSince      map[types.UID]time.Time
	evictionBlockedSince map[types.UID]time.Time
	rejections           map[types.UID]resizeRejection
}

func newResizeTracker() *resizeTracker {
	return &resizeTracker{
		notResizedSince:      make(map[types.UID]time.Time),
		evictionBlockedSince: make(map[types.UID]time.Time),
		rejections:           make(map[types.UID]resizeRejection),
	}
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
	delete(t.evictionBlockedSince, uid)
	delete(t.rejections, uid)
}

// setRejection marks a pod as permanently rejected. It returns true the
// first time a given (uid, reason) pair is recorded, so the caller can warn
// once per pod instead of on every poll.
func (t *resizeTracker) setRejection(uid types.UID, reason resizeRejection) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if prev, ok := t.rejections[uid]; ok {
		return prev != reason
	}
	t.rejections[uid] = reason
	return true
}

// rejection returns the recorded permanent rejection reason for a pod, if any.
func (t *resizeTracker) rejection(uid types.UID) (resizeRejection, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.rejections[uid]
	return r, ok
}

// markEvictionBlocked records the first time a pod's eviction was blocked
// by a PodDisruptionBudget. Returns the timestamp (existing or new).
func (t *resizeTracker) markEvictionBlocked(uid types.UID, now time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if first, ok := t.evictionBlockedSince[uid]; ok {
		return first
	}
	t.evictionBlockedSince[uid] = now
	return now
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
	for uid := range t.evictionBlockedSince {
		if !live[uid] {
			delete(t.evictionBlockedSince, uid)
		}
	}
	for uid := range t.rejections {
		if !live[uid] {
			delete(t.rejections, uid)
		}
	}
}

// buildResizePatch produces a strategic-merge patch body that brings the
// pod's containers to `desired`. It returns (nil, false) if nothing needs
// to change, which lets the caller skip the API call entirely.
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
func classifyResize(pod *v1.Pod) resizeStatus {
	// Scan every condition and keep the most severe state, so the result does
	// not depend on the order of pod.Status.Conditions. The two resize
	// conditions are mutually exclusive in normal kubelet behaviour; computing a
	// precedence is defensive against a pod ever carrying conflicting ones.
	worst := resizeStatusOK
	consider := func(s resizeStatus) {
		if s > worst {
			worst = s
		}
	}
	for _, c := range pod.Status.Conditions {
		if c.Status != v1.ConditionTrue {
			continue
		}
		switch c.Type {
		case v1.PodResizePending:
			if c.Reason == v1.PodReasonInfeasible {
				consider(resizeStatusInfeasible)
			} else {
				consider(resizeStatusDeferred)
			}
		case v1.PodResizeInProgress:
			if c.Reason == v1.PodReasonError {
				// An actuation error will not resolve on its own, so treat it
				// like Infeasible: it must accumulate toward the fallback grace
				// period rather than be classified OK.
				consider(resizeStatusInfeasible)
			} else {
				consider(resizeStatusInProgress)
			}
		}
	}
	return worst
}

// actuation describes whether the kubelet has actually enacted the desired
// resources on the running containers.
type actuation int

const (
	actuationConfirmed actuation = iota // status matches desired for all managed, running containers
	actuationMismatch                   // kubelet reported resources and they differ from desired
	actuationUnknown                    // kubelet has not reported (cs.Resources == nil)
)

// actuationState inspects containerStatuses[].Resources to verify that the
// kubelet has actuated the desired resize. A container that has not started
// yet or is not managed by cpvpa is ignored.
func actuationState(pod *v1.Pod, desired map[string]v1.ResourceRequirements) actuation {
	sawUnknown := false
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		want, managed := desired[cs.Name]
		if !managed || cs.State.Running == nil {
			continue // not started yet: spec applies at container start
		}
		if cs.Resources == nil {
			sawUnknown = true
			continue
		}
		if !resourcesSatisfied(*cs.Resources, want) {
			return actuationMismatch
		}
	}
	if sawUnknown {
		return actuationUnknown
	}
	return actuationConfirmed
}

// EnsureResizeSubresource checks that the cluster supports the pods/resize
// subresource in its 1.33+ (beta) shape. Call at startup when resize mode is
// not Recreate to fail fast with a clear message.
//
// Note: discovery (the API server advertising the subresource) cannot confirm
// the InPlacePodVerticalScaling feature gate is enabled on every kubelet;
// that is detected per pod at runtime (containerStatuses[].resources). The
// version check rejects 1.32's alpha API, which used the deprecated
// pod.status.resize field instead of the PodResizePending/InProgress
// conditions this controller relies on.
func EnsureResizeSubresource(client kubernetes.Interface) error {
	serverVersion, err := client.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("failed to discover server version: %w", err)
	}
	ver, err := utilversion.ParseGeneric(serverVersion.GitVersion)
	if err != nil {
		return fmt.Errorf("failed to parse server version %q: %w", serverVersion.GitVersion, err)
	}
	if ver.Major() < 1 || (ver.Major() == 1 && ver.Minor() < 33) {
		return fmt.Errorf("in-place pod resize requires Kubernetes 1.33+ (server is %s); "+
			"the 1.32 alpha resize API lacks the PodResizePending/InProgress conditions this controller relies on",
			serverVersion.GitVersion)
	}
	resources, err := client.Discovery().ServerResourcesForGroupVersion("v1")
	if err != nil {
		return fmt.Errorf("failed to discover v1 API resources: %w", err)
	}
	for _, r := range resources.APIResources {
		if r.Name == "pods/resize" {
			return nil
		}
	}
	return fmt.Errorf("cluster does not support pods/resize subresource; "+
		"in-place pod resize requires Kubernetes 1.33+ with InPlacePodVerticalScaling enabled (server is %s)",
		serverVersion.GitVersion)
}
