/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package e2e

import (
	"context"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// configFor builds a cpvpa default-config that makes the controller compute a
// constant target (step omitted => independent of node count). This is the
// trigger: cpvpa will drive the hamster to exactly these values.
func configFor(r resources) string {
	var reqs []string
	if r.cpuReq != "" {
		reqs = append(reqs, `"cpu":{"base":"`+r.cpuReq+`"}`)
	}
	if r.memReq != "" {
		reqs = append(reqs, `"memory":{"base":"`+r.memReq+`"}`)
	}
	var lims []string
	if r.cpuLim != "" {
		lims = append(lims, `"cpu":{"base":"`+r.cpuLim+`"}`)
	}
	if r.memLim != "" {
		lims = append(lims, `"memory":{"base":"`+r.memLim+`"}`)
	}
	parts := `"requests":{` + strings.Join(reqs, ",") + `}`
	if len(lims) > 0 {
		parts += `,"limits":{` + strings.Join(lims, ",") + `}`
	}
	return `{"` + hamsterCtr + `":{` + parts + `}}`
}

var _ = ginkgo.Describe("in-place resize", func() {
	var (
		ctx context.Context
		ns  string
	)

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		ns = createNamespace(ctx)
	})
	ginkgo.AfterEach(func() {
		deleteNamespace(ctx, ns)
	})

	ginkgo.It("upsizes a running pod live, without restart or recreation", func() {
		deployHamster(ctx, ns, 1, resources{cpuReq: "100m", cpuLim: "200m", memReq: "64Mi", memLim: "128Mi"})
		pod := singleHamsterPod(ctx, ns)
		uid, restarts := pod.UID, restartCount(&pod)

		deployCPVPA(ctx, ns, cpvpaOpts{
			mode:   "InPlace",
			config: configFor(resources{cpuReq: "300m", cpuLim: "500m", memReq: "64Mi", memLim: "128Mi"}),
		})

		gomega.Eventually(func(g gomega.Gomega) {
			p := getPod(ctx, ns, pod.Name)
			g.Expect(p.UID).To(gomega.Equal(uid), "pod was recreated, not resized in place")
			g.Expect(restartCount(p)).To(gomega.Equal(restarts), "container restarted")
			g.Expect(appliedCPUReqMilli(p)).To(gomega.BeEquivalentTo(300), "status did not reflect the new request")
			g.Expect(cpuMaxMilli(ctx, ns, p.Name)).To(gomega.BeEquivalentTo(500), "cgroup cpu.max not updated in kernel")
		}, 90*time.Second, 3*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("downsizes CPU live on every supported version", func() {
		deployHamster(ctx, ns, 1, resources{cpuReq: "500m", cpuLim: "800m", memReq: "64Mi", memLim: "128Mi"})
		pod := singleHamsterPod(ctx, ns)
		uid, restarts := pod.UID, restartCount(&pod)

		deployCPVPA(ctx, ns, cpvpaOpts{
			mode:   "InPlace",
			config: configFor(resources{cpuReq: "200m", cpuLim: "300m", memReq: "64Mi", memLim: "128Mi"}),
		})

		gomega.Eventually(func(g gomega.Gomega) {
			p := getPod(ctx, ns, pod.Name)
			g.Expect(p.UID).To(gomega.Equal(uid))
			g.Expect(restartCount(p)).To(gomega.Equal(restarts))
			g.Expect(appliedCPUReqMilli(p)).To(gomega.BeEquivalentTo(200))
			g.Expect(cpuMaxMilli(ctx, ns, p.Name)).To(gomega.BeEquivalentTo(300))
		}, 90*time.Second, 3*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("downsizes a memory limit live on GA clusters (idle workload)", func() {
		skipBelow(35, "memory limit decrease only permitted at GA")
		// busybox sleep uses a few MB, well under 64Mi, so the kubelet's
		// usage<newLimit check passes and the decrease is granted live.
		deployHamster(ctx, ns, 1, resources{cpuReq: "100m", cpuLim: "200m", memReq: "64Mi", memLim: "256Mi"})
		pod := singleHamsterPod(ctx, ns)
		uid := pod.UID

		deployCPVPA(ctx, ns, cpvpaOpts{
			mode:   "InPlace",
			config: configFor(resources{cpuReq: "100m", cpuLim: "200m", memReq: "64Mi", memLim: "64Mi"}),
		})

		gomega.Eventually(func(g gomega.Gomega) {
			p := getPod(ctx, ns, pod.Name)
			g.Expect(p.UID).To(gomega.Equal(uid))
			g.Expect(memMaxBytes(ctx, ns, p.Name)).To(gomega.BeEquivalentTo(64 * 1024 * 1024))
		}, 90*time.Second, 3*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("falls back to recreate when a resize is Infeasible (InPlaceOrRecreate)", ginkgo.Label("slow"), func() {
		// Version-robust way to force Infeasible: request far more CPU than
		// the small worker can ever grant. Independent of the memory-decrease
		// rules, so this exercises the fallback path on 1.33 through GA.
		deployHamster(ctx, ns, 1, resources{cpuReq: "100m", cpuLim: "200m"})
		old := singleHamsterPod(ctx, ns)

		deployCPVPA(ctx, ns, cpvpaOpts{
			mode:    "InPlaceOrRecreate",
			config:  configFor(resources{cpuReq: "1000", cpuLim: "1000"}), // 1000 cores
			grace:   "20s",
			maxPods: 1,
		})

		// First: the pod should be marked Infeasible and NOT yet deleted.
		gomega.Eventually(func(g gomega.Gomega) {
			p := getPod(ctx, ns, old.Name)
			g.Expect(resizeCondition(p, corev1.PodResizePending)).To(gomega.Equal("Infeasible"))
		}, 60*time.Second, 3*time.Second).Should(gomega.Succeed())

		// Then, after the grace period, the old pod is gone and a fresh pod
		// exists (recreated by the ReplicaSet). New pod likely Pending — the
		// 1000-core request is unschedulable — which is the expected, honest
		// outcome of "give up on in-place, let the controller try".
		gomega.Eventually(func(g gomega.Gomega) {
			_, err := env.clientset.CoreV1().Pods(ns).Get(ctx, old.Name, metav1.GetOptions{})
			g.Expect(err).To(gomega.HaveOccurred(), "old Infeasible pod should have been evicted")
			// During a rolling update the old ReplicaSet may briefly keep
			// recreating a pod because the new pod (unschedulable) never
			// becomes Ready. Assert at least one pod exists, not exactly one.
			g.Expect(hamsterPods(ctx, ns)).To(gomega.Not(gomega.BeEmpty()),
				"ReplicaSet should have recreated at least one pod")
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("respects MaxPodsPerCycle on a DaemonSet during fallback", ginkgo.Label("slow"), func() {
		// DaemonSet variant: with several Infeasible pods, only one is
		// evicted per poll cycle. (Single-node kind => one DS pod; for a
		// real multi-pod assertion this spec is marked multi-node.)
		ginkgo.Skip("requires a multi-worker cluster; see kind-config-multi.yaml job")
	})

	ginkgo.It("does NOT churn when cpvpa manages only a subset of resources", func() {
		// Regression for the partial-management bug: container has limits,
		// cpvpa config sets only requests. After it converges once, it must
		// recognize the pod as already-satisfied and stop issuing resizes.
		//
		// A no-op /resize is invisible on the object (resourceVersion is
		// stable either way), so we assert on cpvpa's own resize-cycle log:
		// buggy => "Applied:1" every cycle; fixed => "AlreadyOK:1".
		deployHamster(ctx, ns, 1, resources{cpuReq: "100m", cpuLim: "500m", memReq: "64Mi", memLim: "256Mi"})

		deployCPVPA(ctx, ns, cpvpaOpts{
			mode:   "InPlace",
			config: configFor(resources{cpuReq: "200m", memReq: "128Mi"}), // requests only
		})

		// Let it converge, then confirm subsequent cycles report AlreadyOK
		// rather than re-Applying. Window covers several poll cycles.
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(appliedCPUReqMilli(getPod(ctx, ns, singleHamsterPod(ctx, ns).Name))).
				To(gomega.BeEquivalentTo(200))
		}, 60*time.Second, 3*time.Second).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			logs := cpvpaLogs(ctx, ns)
			g.Expect(logs).To(gomega.ContainSubstring("AlreadyOK:1"),
				"cpvpa never recognized the pod as already-satisfied (partial-management churn)")
		}, 60*time.Second, 5*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("does not touch live pods in Recreate mode", ginkgo.Label("slow"), func() {
		deployHamster(ctx, ns, 1, resources{cpuReq: "100m", cpuLim: "200m"})
		old := singleHamsterPod(ctx, ns)

		deployCPVPA(ctx, ns, cpvpaOpts{
			mode:   "Recreate",
			config: configFor(resources{cpuReq: "300m", cpuLim: "500m"}),
		})

		// The template is patched and the ReplicaSet rolls: the old pod is
		// replaced (new UID), NOT resized in place.
		gomega.Eventually(func(g gomega.Gomega) {
			pods := hamsterPods(ctx, ns)
			g.Expect(pods).To(gomega.HaveLen(1))
			g.Expect(pods[0].UID).NotTo(gomega.Equal(old.UID), "Recreate must roll the pod, not resize it")
			g.Expect(appliedCPUReqMilli(&pods[0])).To(gomega.BeEquivalentTo(300))
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed())
	})
})

// resizeCondition returns the Reason of the given resize condition type, or ""
// if absent. Used to detect Deferred/Infeasible.
func resizeCondition(pod *corev1.Pod, t corev1.PodConditionType) string {
	for _, c := range pod.Status.Conditions {
		if c.Type == t {
			return c.Reason
		}
	}
	return ""
}
