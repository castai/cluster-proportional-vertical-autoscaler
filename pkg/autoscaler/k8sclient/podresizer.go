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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
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
	// period, the pod is deleted so the owning controller can recreate it.
	// For self-healing targets that goes through a template patch and the
	// controller's rollout; for the rest cpvpa disrupts the pod directly.
	// Node-bound targets (DaemonSets) are exempt: their pods are recreated
	// on the same node, so a pod that does not fit would only be replaced
	// by a Pending twin.
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
	// rejectInvalidOther: the API server refused the resize as Invalid for a
	// reason not recognized above. Its cause is logged with the warning.
	rejectInvalidOther resizeRejection = "invalid-other"
)

// podStable reports whether the rejection depends only on the pod (and its
// node), not on the values cpvpa wants to apply. Pod-stable rejections hold
// for the pod's lifetime; the rest (QoS change, request above limit) are
// re-evaluated as soon as the desired values change.
func (r resizeRejection) podStable() bool {
	switch r {
	case rejectNodeUnsupported, rejectPodLevel, rejectStaticPod, rejectWindows:
		return true
	}
	return false
}

// logWarningf is the warning sink for once-per-pod rejection warnings;
// a variable so tests can count emissions.
var logWarningf = glog.Warningf

// FieldManager is the field manager cpvpa records for every write, so its
// ownership of pod and template fields is visible in managedFields and can be
// told apart from other resizers'.
const FieldManager = "cpvpa"

// ConflictPolicy decides what cpvpa does when another manager resized a pod
// field cpvpa manages.
type ConflictPolicy string

const (
	// ConflictYield (default) leaves the pod alone while the desired values
	// stay the same, and takes the fields back once when they change. A
	// yielded pod never enters the recreation fallback.
	ConflictYield ConflictPolicy = "yield"
	// ConflictOverride always re-applies cpvpa's values (the pre-detection
	// behavior). With another active resizer, both revert each other.
	ConflictOverride ConflictPolicy = "override"
)

// admissionManager marks a conflict entry for a /resize that an admission
// webhook mutated: the result is attributed to cpvpa, so ownership can't
// reveal it.
const admissionManager = "(admission webhook)"

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
	// NodeBound reports whether the target's pods are pinned to their node
	// (DaemonSet semantics): a recreated pod asks the SAME node for the same
	// resources, so recreation cannot fix a pod that does not fit.
	NodeBound() bool
	// RecreatesAtNewSize reports whether a pod deleted by the fallback is
	// recreated from the current template (false for a paused Deployment or
	// a partitioned StatefulSet).
	RecreatesAtNewSize(ctx context.Context) (bool, error)
}

// targetTraits are the per-target facts the fallback needs.
type targetTraits struct {
	nodeBound    bool // pods are recreated on the same node
	recreatesNew bool // a deleted pod comes back from the current template
}

// podResizer orchestrates in-place pod resizing and recreation fallback.
type podResizer struct {
	resizeMode     ResizeMode
	fallbackConfig ResizeFallbackConfig
	dryRun         bool
	clock          clock.PassiveClock
	clientset      kubernetes.Interface
	tracker        *resizeTracker
	onConflict     ConflictPolicy // "" means ConflictYield
}

type resizeResult struct {
	TargetPods               int // pods owned by the target that we considered
	AlreadyOK                int // pods already at the desired resources
	Applied                  int // /resize patches accepted by the API server
	InProgress               int // kubelet has accepted and is applying
	Deferred                 int // kubelet says: not now, maybe later (node pressure)
	Infeasible               int // kubelet says: never on this node
	ActuationLag             int // kubelet reported resources but they differ from desired
	Evicted                  int // pods cpvpa evicted or deleted (fallback)
	RecreateTriggered        int // pods handed to the controller's rollout via a template patch
	RecreateStuck            int // self-healing pods still not resized though the template already matches desired (rollout wedged, likely unschedulable)
	EvictionBlocked          int // pods whose eviction was blocked by PDB
	FallbackSkippedNodeBound int // pods whose fallback disruption was skipped: node-bound target + Deferred/Infeasible (recreation cannot fix them)
	FallbackSkippedOldSize   int // pods whose fallback disruption was skipped: the controller would recreate them at the old size (paused Deployment, partitioned StatefulSet)
	ExternalConflict         int // pods whose managed fields another manager (or an admission webhook) set to other values
	Rejected                 int // pods whose resize was skipped: permanently rejected (see tracker rejection reasons)
	Unsupported              int // subset of Rejected: pods on nodes whose kubelet lacks InPlacePodVerticalScaling
	ActuationUnknown         int // pods at desired spec whose kubelet reports no status resources (node without in-place support); treated as converged
	WaitingToStart           int // scheduled pods skipped until at least one container is running
	Transient                int // transient errors (404 NotFound, 409 Conflict) per pod
	Errors                   int // any other unexpected error per pod
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
	traits := targetTraits{nodeBound: target.NodeBound(), recreatesNew: true}
	if r.resizeMode == ResizeModeInPlaceOrRecreate {
		traits.recreatesNew, err = target.RecreatesAtNewSize(ctx)
		if err != nil {
			return result, fmt.Errorf("recreate check: %w", err)
		}
	}
	desiredKey := resourcesKey(desired)

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

		// A scheduled pod whose containers have never started is picked up on
		// a later poll: its node's resize support can't be told yet, and the
		// container will start from the latest spec anyway. Unscheduled pods
		// are patched (the scheduler uses the new values), and so are pods
		// whose containers ran before but are not running now (crash loop):
		// the API server accepts that resize, and the kubelet applies it on
		// the next restart — often exactly the fix an OOM-looping pod needs.
		if pod.Spec.NodeName != "" && !hasRunningContainer(pod) && !hasStartedBefore(pod) {
			result.WaitingToStart++
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
					r.maybeFallbackEvict(ctx, pod, now, age, selfHealing, resizeStatusOK, desired, traits, ensureTemplate, &evictedThisCycle, &result)
				case actuationUnknown:
					// A running container with nil status.resources: the node's
					// kubelet has no in-place support, so it cannot confirm
					// actuation. But the spec already matches desired, and such a
					// kubelet runs containers from the spec, so there is nothing
					// to do. It must NOT be treated as stuck: the fallback would
					// recreate a correctly sized pod, and its replacement (same
					// size, same kind of node) again, every grace period.
					result.ActuationUnknown++
					r.tracker.clear(pod.UID)
				}
			} else {
				r.accountNotResized(ctx, pod, status, now, selfHealing, desired, traits, ensureTemplate, &evictedThisCycle, &result)
			}
			continue
		}

		// Another resizer (or a webhook) set the fields cpvpa wants to change.
		// Checked before rejections: a yielded pod must never reach the
		// recreation fallback, which would evict it over someone else's sizing.
		if r.yieldToOtherManager(pod, desired, desiredKey, &result) {
			continue
		}

		// Only a pod that still needs a resize can have it rejected; checking
		// before buildResizePatch would mark correctly sized pods as stuck.
		// A pod whose resize is permanently rejected (determined this poll or
		// on an earlier one) is skipped; the fallback machinery still sees it
		// so InPlaceOrRecreate can converge it via recreation, which does not
		// depend on in-place support.
		if reason, rejected := r.tracker.rejection(pod.UID, desiredKey); rejected {
			r.noteRejection(ctx, pod, reason, "", now, selfHealing, desired, traits, ensureTemplate, &evictedThisCycle, &result)
			continue
		}
		if reason, rejected := precheckResize(pod, desired); rejected {
			r.noteRejection(ctx, pod, reason, "", now, selfHealing, desired, traits, ensureTemplate, &evictedThisCycle, &result)
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
			metav1.PatchOptions{FieldManager: FieldManager},
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
				// Every Invalid response is a rejection: the same PATCH would
				// fail again, and it says nothing about whether the size fits
				// the node. Unrecognized causes are re-checked when the
				// desired values change, like other desired-dependent ones.
				reason, cause := rejectionReasonFromError(patchErr)
				r.noteRejection(ctx, pod, reason, cause, now, selfHealing, desired, traits, ensureTemplate, &evictedThisCycle, &result)
				continue
			}
			result.Errors++
			glog.Errorf("resize patch error for pod=%s/%s: %v",
				pod.Namespace, pod.Name, patchErr)
			continue
		} else {
			result.Applied++
			r.tracker.finishTakeBack(updated.UID)
			// The API server accepted the PATCH, but an admission webhook can
			// still have changed the values. Re-sending would be mutated the
			// same way every poll; wait for the desired values to change.
			if _, stillDiffers := buildResizePatch(updated, desired); stillDiffers {
				r.tracker.clearClock(updated.UID)
				result.ExternalConflict++
				if r.tracker.setConflict(updated.UID, admissionManager, desiredKey) {
					logWarningf("pod=%s/%s: the /resize was accepted but its resources were changed on admission (desired %s, got %s); "+
						"not re-sending until the desired resources change",
						updated.Namespace, updated.Name, formatResourceRequests(desired), formatPodRequests(updated, desired))
				}
				continue
			}
			// A successful patch is a fresh resize attempt, so normally the
			// fallback clock restarts. Not when the pod was already reported
			// as not fitting (Deferred/Infeasible) and no value went down:
			// the new size is no easier to fit, and resetting would let a
			// node count flapping across a step boundary postpone the
			// fallback forever.
			if s := classifyResize(pod); (s == resizeStatusDeferred || s == resizeStatusInfeasible) && noneDecreased(pod, desired) {
				r.tracker.keepClock(updated.UID)
			} else {
				r.tracker.clear(updated.UID)
			}
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
				r.maybeFallbackEvict(ctx, updated, now, age, selfHealing, resizeStatusOK, desired, traits, ensureTemplate, &evictedThisCycle, &result)
			case actuationUnknown:
				// Spec now matches desired; see the unpatched-pod case above.
				result.ActuationUnknown++
				r.tracker.clear(updated.UID)
			}
		} else {
			r.accountNotResized(ctx, updated, status, now, selfHealing, desired, traits,
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
	desired map[string]v1.ResourceRequirements,
	traits targetTraits,
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
	case resizeStatusRejected:
		// counted as Rejected by noteRejection
	}
	firstSeen := r.tracker.markNotResized(pod.UID, now)
	age := now.Sub(firstSeen)
	glog.V(2).Infof("pod=%s/%s resize %s (not resized for %s)", pod.Namespace, pod.Name, status, age)
	r.maybeFallbackEvict(ctx, pod, now, age, selfHeals, status, desired, traits, ensureTemplate, evictedThisCycle, result)
}

// yieldToOtherManager handles a pod whose fields cpvpa wants to change were
// last set by someone else, and reports whether to skip the pod.
//
//   - A pod already yielded for the same desired values stays yielded (an
//     admission-webhook entry in either policy, an external resizer under
//     ConflictYield). It leaves the fallback clock.
//   - Once the desired values change, cpvpa takes the fields back once: the
//     entry is dropped and the pod is patched without re-checking ownership.
//   - Otherwise, if another manager owns a differing field, the conflict is
//     recorded and warned about once; ConflictYield skips the pod,
//     ConflictOverride patches it anyway.
func (r *podResizer) yieldToOtherManager(pod *v1.Pod, desired map[string]v1.ResourceRequirements, desiredKey string, result *resizeResult) bool {
	yield := r.onConflict != ConflictOverride
	if e, ok := r.tracker.conflict(pod.UID); ok {
		switch {
		case e.takingBack && e.desiredKey == desiredKey:
			// The take-back PATCH hasn't succeeded yet (conflict, not found,
			// dry-run): keep trying. A successful PATCH ends it.
			return false
		case e.desiredKey != desiredKey && e.manager != admissionManager && yield:
			r.tracker.startTakeBack(pod.UID, e.manager, desiredKey)
			glog.Infof("pod=%s/%s: desired resources changed, taking the fields back from %q once",
				pod.Namespace, pod.Name, e.manager)
			return false
		case e.desiredKey != desiredKey:
			r.tracker.clearConflict(pod.UID)
		case e.manager == admissionManager || yield:
			result.ExternalConflict++
			r.tracker.clearClock(pod.UID)
			return true
		}
	}
	manager, field := externalResizeOwner(pod, desired)
	if manager == "" {
		return false
	}
	result.ExternalConflict++
	if r.tracker.setConflict(pod.UID, manager, desiredKey) {
		action := "leaving it alone until the desired resources change"
		if !yield {
			action = "overriding it (--resize-on-conflict=override)"
		}
		logWarningf("pod=%s/%s: %s was resized by %q (desired %s, pod has %s); %s. "+
			"cpvpa should be the only resizer of the containers it manages",
			pod.Namespace, pod.Name, field, manager, formatResourceRequests(desired), formatPodRequests(pod, desired), action)
	}
	if yield {
		r.tracker.clearClock(pod.UID)
	}
	return yield
}

// externalResizeOwner returns the manager, other than cpvpa, that last set
// one of the pod's resource fields cpvpa wants to change through the resize
// subresource, plus that field, or "" if there is none. Ownership comes from
// managedFields: it is per field, moves to whoever last changed the value,
// and is recorded with subresource "resize" for in-place resizes. Owners
// without that subresource (the pod's creator, e.g. kube-controller-manager)
// set the creation-time values and are not resizers.
func externalResizeOwner(pod *v1.Pod, desired map[string]v1.ResourceRequirements) (manager, field string) {
	type candidate struct {
		path  fieldpath.Path
		label string
	}
	var differing []candidate
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		want, managed := desired[c.Name]
		if !managed {
			continue
		}
		for _, list := range []struct {
			kind       string
			have, want v1.ResourceList
		}{{"requests", c.Resources.Requests, want.Requests}, {"limits", c.Resources.Limits, want.Limits}} {
			for res, w := range list.want {
				if h, ok := list.have[res]; ok && h.Cmp(w) == 0 {
					continue
				}
				differing = append(differing, candidate{
					path: fieldpath.MakePathOrDie("spec", "containers", fieldpath.KeyByFields("name", c.Name),
						"resources", list.kind, string(res)),
					label: fmt.Sprintf("container %q %s.%s", c.Name, list.kind, res),
				})
			}
		}
	}
	if len(differing) == 0 {
		return "", ""
	}
	for _, mf := range pod.ManagedFields {
		if mf.Subresource != "resize" || mf.Manager == FieldManager || mf.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		if err := set.FromJSON(bytes.NewReader(mf.FieldsV1.Raw)); err != nil {
			continue
		}
		for _, d := range differing {
			if set.Has(d.path) {
				return mf.Manager, d.label
			}
		}
	}
	return "", ""
}

// formatPodRequests renders the pod's current requests for the containers in
// desired, in the same shape as formatResourceRequests.
func formatPodRequests(pod *v1.Pod, desired map[string]v1.ResourceRequirements) string {
	current := make(map[string]v1.ResourceRequirements, len(desired))
	for i := range pod.Spec.Containers {
		if _, managed := desired[pod.Spec.Containers[i].Name]; managed {
			current[pod.Spec.Containers[i].Name] = pod.Spec.Containers[i].Resources
		}
	}
	return formatResourceRequests(current)
}

// noteRejection accounts for one pod whose resize is rejected: it records
// the rejection (it is the only caller of setRejection, so the once-per-(pod,
// reason) warning fires exactly once), counts it, and feeds the pod to the
// fallback clock with resizeStatusRejected. That status is deliberately not
// Infeasible: a rejection says nothing about whether the new size fits the
// node, so the node-bound guard must not block recreation for it.
func (r *podResizer) noteRejection(
	ctx context.Context,
	pod *v1.Pod,
	reason resizeRejection,
	cause string, // API server message, when the rejection came from a 422
	now time.Time,
	selfHeals bool,
	desired map[string]v1.ResourceRequirements,
	traits targetTraits,
	ensureTemplate func() (bool, error),
	evictedThisCycle *int,
	result *resizeResult,
) {
	result.Rejected++
	if reason == rejectNodeUnsupported {
		result.Unsupported++
	}
	if r.tracker.setRejection(pod.UID, reason, resourcesKey(desired)) {
		if cause != "" {
			defer logWarningf("pod=%s/%s resize rejected by the API server: %s", pod.Namespace, pod.Name, cause)
		}
		if reason == rejectNodeUnsupported {
			logWarningf("pod=%s/%s on node %q cannot resize in place: kubelet has no InPlacePodVerticalScaling support",
				pod.Namespace, pod.Name, pod.Spec.NodeName)
		} else if reason.podStable() {
			logWarningf("pod=%s/%s resize permanently rejected (%s); skipping /resize patches for it",
				pod.Namespace, pod.Name, reason)
		} else {
			logWarningf("pod=%s/%s resize rejected (%s); skipping /resize patches for it until the desired resources change",
				pod.Namespace, pod.Name, reason)
		}
	}
	r.accountNotResized(ctx, pod, resizeStatusRejected, now, selfHeals, desired, traits, ensureTemplate, evictedThisCycle, result)
}

// hasActuationError reports whether the kubelet reported a resize actuation
// error (PodResizeInProgress with reason Error). The pod's current allocation
// already fits its node, so recreating a node-bound pod is safe in that case.
func hasActuationError(pod *v1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == v1.PodResizeInProgress && c.Status == v1.ConditionTrue && c.Reason == v1.PodReasonError {
			return true
		}
	}
	return false
}

// formatResourceRequests renders the desired per-container requests compactly
// for warning logs, e.g. "main: cpu=200m memory=256Mi".
func formatResourceRequests(desired map[string]v1.ResourceRequirements) string {
	names := make([]string, 0, len(desired))
	for name := range desired {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		for _, res := range []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory} {
			if q, ok := desired[name].Requests[res]; ok {
				parts = append(parts, fmt.Sprintf("%s: %s=%s", name, res, q.String()))
			}
		}
	}
	return strings.Join(parts, ", ")
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

// noneDecreased reports whether, for every container and resource cpvpa
// manages, the desired request and limit are at least the pod's current
// spec value (a value the pod does not set yet counts as an increase).
func noneDecreased(pod *v1.Pod, desired map[string]v1.ResourceRequirements) bool {
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		want, managed := desired[c.Name]
		if !managed {
			continue
		}
		for _, pair := range []struct{ have, want v1.ResourceList }{
			{c.Resources.Requests, want.Requests},
			{c.Resources.Limits, want.Limits},
		} {
			for name, w := range pair.want {
				if h, ok := pair.have[name]; ok && w.Cmp(h) < 0 {
					return false
				}
			}
		}
	}
	return true
}

// hasStartedBefore reports whether any container of the pod has run before
// (restarted, or has a terminated last state) — e.g. a crash-looping pod
// caught between restarts.
func hasStartedBefore(pod *v1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.RestartCount > 0 || cs.LastTerminationState.Terminated != nil {
			return true
		}
	}
	return false
}

// resourcesKey renders desired resources (requests and limits, every
// container) as a deterministic string, used to tell whether a recorded
// desired-dependent rejection still applies.
func resourcesKey(desired map[string]v1.ResourceRequirements) string {
	names := make([]string, 0, len(desired))
	for name := range desired {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	list := func(tag string, rl v1.ResourceList) {
		keys := make([]string, 0, len(rl))
		for k := range rl {
			keys = append(keys, string(k))
		}
		sort.Strings(keys)
		for _, k := range keys {
			q := rl[v1.ResourceName(k)]
			fmt.Fprintf(&b, "%s.%s=%s,", tag, k, q.String())
		}
	}
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte('{')
		list("req", desired[name].Requests)
		list("lim", desired[name].Limits)
		b.WriteString("};")
	}
	return b.String()
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
	// The QoS class counts init containers too (unchanged by a resize). The
	// API server compares the new class with the pod's recorded one.
	oldQoS := pod.Status.QOSClass
	if oldQoS == "" {
		oldQoS = podQoS(append(append([]v1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...))
	}
	if oldQoS != podQoS(append(append([]v1.Container{}, pod.Spec.InitContainers...), mergedContainers(pod, desired)...)) {
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

// podQoS computes a QoS class over the given containers (cpu and memory)
// following the upstream algorithm: Guaranteed when every container sets
// cpu+memory requests equal to limits, BestEffort when nothing is set,
// Burstable otherwise. Callers pass init containers too, as upstream does.
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

// rejectionReasonFromError maps an API Invalid (422) response onto a
// rejection reason, mirroring ValidatePodResize's messages, and returns the
// API server's messages joined for logging. Unrecognized causes map to
// rejectInvalidOther.
func rejectionReasonFromError(err error) (resizeRejection, string) {
	statusErr, ok := err.(apierrors.APIStatus)
	if !ok {
		return rejectInvalidOther, err.Error()
	}
	st := statusErr.Status()
	msgs := make([]string, 0, 1)
	if st.Message != "" {
		msgs = append(msgs, st.Message)
	}
	if st.Details != nil {
		for _, c := range st.Details.Causes {
			if c.Message != "" {
				msgs = append(msgs, c.Message)
			}
		}
	}
	cause := strings.Join(msgs, "; ")
	if cause == "" {
		cause = err.Error()
	}
	for _, m := range msgs {
		switch {
		case strings.Contains(m, "without support for resize"):
			return rejectNodeUnsupported, cause
		case strings.Contains(m, "QOS Class may not change"):
			return rejectQoSChange, cause
		case strings.Contains(m, "less than or equal to"):
			return rejectAboveLimit, cause
		}
	}
	return rejectInvalidOther, cause
}

// maybeFallbackEvict handles the InPlaceOrRecreate fallback for one not-resized pod.
func (r *podResizer) maybeFallbackEvict(
	ctx context.Context,
	pod *v1.Pod,
	now time.Time,
	age time.Duration,
	selfHeals bool,
	status resizeStatus,
	desired map[string]v1.ResourceRequirements,
	traits targetTraits,
	ensureTemplate func() (bool, error),
	evictedThisCycle *int,
	result *resizeResult,
) {
	if r.resizeMode != ResizeModeInPlaceOrRecreate || age < r.fallbackConfig.GracePeriod {
		return
	}
	// Node-bound targets (DaemonSets): a disrupted pod is recreated on the
	// same node, so a Deferred/Infeasible resize would only replace a working
	// agent with a Pending twin. Actuation errors are exempt — the pod's
	// current allocation already fits the node, so the replacement can start.
	// So are rejections (resizeStatusRejected), e.g. a kubelet without
	// in-place support: recreation is the only way such a pod converges.
	if traits.nodeBound && (status == resizeStatusDeferred ||
		(status == resizeStatusInfeasible && !hasActuationError(pod))) {
		result.FallbackSkippedNodeBound++
		if r.tracker.firstFallbackSkip(pod.UID) {
			msg := fmt.Sprintf("pod=%s/%s on node-bound target: not recreating %s pod on node %q — "+
				"the replacement would ask the same node for the same resources (desired %s) and stay Pending",
				pod.Namespace, pod.Name, status, pod.Spec.NodeName, formatResourceRequests(desired))
			if node, err := r.clientset.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{}); err == nil {
				msg += fmt.Sprintf("; node allocatable: cpu=%s memory=%s",
					node.Status.Allocatable.Cpu().String(), node.Status.Allocatable.Memory().String())
			}
			glog.Error(msg)
		}
		return
	}
	// A paused Deployment or a partitioned StatefulSet would recreate the
	// pod from the old template (old ReplicaSet / current revision): it
	// would come back at the old size and be disrupted again every grace
	// period. And patching the template would roll pods when the rollout is
	// resumed. Leave it to whoever paused it.
	if !traits.recreatesNew {
		result.FallbackSkippedOldSize++
		if r.tracker.firstFallbackSkip(pod.UID) {
			logWarningf("pod=%s/%s not resized for %s, but not recreating it: the target is paused or partitioned, "+
				"so its controller would recreate it at the old size (desired %s)",
				pod.Namespace, pod.Name, age, formatResourceRequests(desired))
		}
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
	rejections           map[types.UID]rejectionEntry
	conflicts            map[types.UID]conflictEntry // kept across clear(): see yieldToOtherManager
	fallbackWarned       map[types.UID]bool
}

func newResizeTracker() *resizeTracker {
	return &resizeTracker{
		notResizedSince:      make(map[types.UID]time.Time),
		evictionBlockedSince: make(map[types.UID]time.Time),
		rejections:           make(map[types.UID]rejectionEntry),
		conflicts:            make(map[types.UID]conflictEntry),
		fallbackWarned:       make(map[types.UID]bool),
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
	delete(t.fallbackWarned, uid)
}

// rejectionEntry is a recorded rejection plus the desired values
// (resourcesKey) it was determined for.
type rejectionEntry struct {
	reason     resizeRejection
	desiredKey string
}

// conflictEntry records that a pod's managed fields were set by another
// manager while cpvpa wanted desiredKey.
type conflictEntry struct {
	manager    string
	desiredKey string
	takingBack bool // desired changed: re-applying once, until a PATCH succeeds
}

// setConflict records a conflict and reports whether it is new for this pod
// or names a different manager (so the caller warns once).
func (t *resizeTracker) setConflict(uid types.UID, manager, desiredKey string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	prev, ok := t.conflicts[uid]
	t.conflicts[uid] = conflictEntry{manager: manager, desiredKey: desiredKey}
	return !ok || prev.manager != manager
}

func (t *resizeTracker) conflict(uid types.UID) (conflictEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.conflicts[uid]
	return e, ok
}

// startTakeBack marks a yielded pod as being taken back for desiredKey.
func (t *resizeTracker) startTakeBack(uid types.UID, manager, desiredKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conflicts[uid] = conflictEntry{manager: manager, desiredKey: desiredKey, takingBack: true}
}

// finishTakeBack drops a take-back entry once its PATCH succeeded. Other
// entries are kept (override mode uses them to warn once).
func (t *resizeTracker) finishTakeBack(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conflicts[uid].takingBack {
		delete(t.conflicts, uid)
	}
}

func (t *resizeTracker) clearConflict(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conflicts, uid)
}

// clearClock takes a pod off the fallback clock without touching its other
// state: a pod cpvpa yields to another manager must never be recreated.
func (t *resizeTracker) clearClock(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.notResizedSince, uid)
	delete(t.evictionBlockedSince, uid)
}

// keepClock handles a successful re-patch that leaves the resize no easier:
// the fallback clock (notResizedSince) is kept, the rest of the pod's state
// (rejection, eviction-blocked timer) is reset as for a fresh attempt.
func (t *resizeTracker) keepClock(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.evictionBlockedSince, uid)
	delete(t.rejections, uid)
}

// setRejection records a pod's rejection for the given desired values. It
// returns true when the pod had no rejection or a different reason, so the
// caller warns once per (pod, reason) instead of on every poll.
func (t *resizeTracker) setRejection(uid types.UID, reason resizeRejection, desiredKey string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	prev, ok := t.rejections[uid]
	t.rejections[uid] = rejectionEntry{reason: reason, desiredKey: desiredKey}
	return !ok || prev.reason != reason
}

// rejection returns the pod's recorded rejection if it still applies:
// pod-stable reasons always do; desired-dependent ones only while the
// desired values are unchanged. A stale entry is kept (not deleted) so a
// repeat rejection for the same reason does not warn again.
func (t *resizeTracker) rejection(uid types.UID, desiredKey string) (resizeRejection, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.rejections[uid]
	if !ok || (!e.reason.podStable() && e.desiredKey != desiredKey) {
		return "", false
	}
	return e.reason, true
}

// firstFallbackSkip reports, once per pod UID, that the fallback declined to
// disrupt the pod (node-bound or paused/partitioned target), so the reason
// is logged once instead of every poll.
func (t *resizeTracker) firstFallbackSkip(uid types.UID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fallbackWarned[uid] {
		return false
	}
	t.fallbackWarned[uid] = true
	return true
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
	for uid := range t.fallbackWarned {
		if !live[uid] {
			delete(t.fallbackWarned, uid)
		}
	}
	for uid := range t.conflicts {
		if !live[uid] {
			delete(t.conflicts, uid)
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
	// resizeStatusRejected is never returned by classifyResize: it marks a
	// pod whose /resize cpvpa skipped (or the API server refused) because it
	// can never succeed as requested. Unlike Infeasible, it carries no
	// node-fit signal.
	resizeStatusRejected
)

func (s resizeStatus) String() string {
	switch s {
	case resizeStatusInProgress:
		return "InProgress"
	case resizeStatusDeferred:
		return "Deferred"
	case resizeStatusInfeasible:
		return "Infeasible"
	case resizeStatusRejected:
		return "Rejected"
	default:
		return "OK"
	}
}

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

// ErrResizeUnsupported is returned (wrapped) by EnsureResizeSubresource when
// discovery answered and the cluster definitively cannot do in-place resize
// (server older than 1.33, or no pods/resize subresource). Any other error
// is a failed discovery call, which says nothing about the cluster.
var ErrResizeUnsupported = errors.New("in-place pod resize unsupported")

// resizeDiscoveryBackoff paces startup retries of a failing discovery call:
// 1s, 2s, 4s, 8s between 5 attempts (~15s) before giving up.
var resizeDiscoveryBackoff = wait.Backoff{Duration: time.Second, Factor: 2, Steps: 5}

// EnsureResizeSubresource checks that the cluster supports the pods/resize
// subresource in its 1.33+ (beta) shape. Call at startup when resize mode is
// not Recreate to fail fast with a clear message. A definitive "no" wraps
// ErrResizeUnsupported; other errors are discovery failures.
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
		return fmt.Errorf("%w: requires Kubernetes 1.33+ (server is %s); "+
			"the 1.32 alpha resize API lacks the PodResizePending/InProgress conditions this controller relies on",
			ErrResizeUnsupported, serverVersion.GitVersion)
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
	return fmt.Errorf("%w: cluster does not serve the pods/resize subresource; "+
		"in-place pod resize requires Kubernetes 1.33+ with InPlacePodVerticalScaling enabled (server is %s)",
		ErrResizeUnsupported, serverVersion.GitVersion)
}

// ensureResizeWithRetry runs EnsureResizeSubresource, retrying discovery
// failures with backoff. A definitive ErrResizeUnsupported answer is
// returned immediately.
func ensureResizeWithRetry(client kubernetes.Interface, backoff wait.Backoff) error {
	var lastErr error
	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		lastErr = EnsureResizeSubresource(client)
		if lastErr == nil {
			return true, nil
		}
		if errors.Is(lastErr, ErrResizeUnsupported) {
			return false, lastErr
		}
		glog.Warningf("in-place resize support check failed, retrying: %v", lastErr)
		return false, nil
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("in-place resize support check kept failing: %w", lastErr)
	}
	return err
}

// resolveResizeMode decides the effective resize mode at startup. InPlace
// requires in-place support and fails otherwise. InPlaceOrRecreate degrades
// to Recreate only when the cluster definitively lacks support; a discovery
// call that keeps failing is returned as an error (the process exits and
// restarts) rather than locking the run into Recreate on a transient blip.
func resolveResizeMode(client kubernetes.Interface, mode ResizeMode, backoff wait.Backoff) (ResizeMode, error) {
	if mode == ResizeModeRecreate {
		return mode, nil
	}
	err := ensureResizeWithRetry(client, backoff)
	switch {
	case err == nil:
		return mode, nil
	case mode == ResizeModeInPlaceOrRecreate && errors.Is(err, ErrResizeUnsupported):
		glog.Warningf("pods/resize unavailable (%v); %s degrading to %s", err, mode, ResizeModeRecreate)
		return ResizeModeRecreate, nil
	case mode == ResizeModeInPlace:
		return mode, fmt.Errorf("in-place resize requires the pods/resize subresource: %w", err)
	default:
		return mode, err
	}
}
