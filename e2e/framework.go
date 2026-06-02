/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

// Package e2e contains end-to-end tests for cpvpa's in-place resize support.
// They run against a real cluster (kind in CI) reachable via KUBECONFIG.
//
// Design notes:
//   - kind cannot add nodes at runtime, so we never scale the cluster. A
//     resize is triggered purely by the gap between a pod's current
//     resources and what cpvpa computes from its config. We make that gap
//     by construction: deploy the hamster at size X, start cpvpa with a
//     constant `base` of Y (step omitted, so node count is irrelevant).
//   - All resize assertions are Eventually/Consistently because the kubelet
//     applies resizes asynchronously and writes status after the fact.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	hamsterImage   = "busybox:1.36"
	hamsterCommand = "sleep 3600"
	hamsterCtr     = "hamster"
	pollPeriodSecs = 2 // tighter than prod 10s to keep specs fast
)

// testEnv is populated by BeforeSuite (see suite_test.go).
type testEnv struct {
	clientset   kubernetes.Interface
	restConfig  *rest.Config
	cpvpaImage  string // e.g. "cpvpa:e2e", loaded into kind
	serverMinor int    // 33, 34, 35, ...
}

var env testEnv

// --- namespaces -------------------------------------------------------------

func createNamespace(ctx context.Context) string {
	name := "cpvpa-e2e-" + rand.String(5)
	_, err := env.clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return name
}

func deleteNamespace(ctx context.Context, ns string) {
	_ = env.clientset.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{})
	// ClusterRole/Binding are cluster-scoped; clean up the ns-suffixed ones.
	_ = env.clientset.RbacV1().ClusterRoles().Delete(ctx, "cpvpa-"+ns, metav1.DeleteOptions{})
	_ = env.clientset.RbacV1().ClusterRoleBindings().Delete(ctx, "cpvpa-"+ns, metav1.DeleteOptions{})
}

// --- the hamster workload ---------------------------------------------------

type resources struct {
	cpuReq, memReq string
	cpuLim, memLim string
}

func (r resources) requirements() corev1.ResourceRequirements {
	req := corev1.ResourceList{}
	lim := corev1.ResourceList{}
	if r.cpuReq != "" {
		req[corev1.ResourceCPU] = resource.MustParse(r.cpuReq)
	}
	if r.memReq != "" {
		req[corev1.ResourceMemory] = resource.MustParse(r.memReq)
	}
	if r.cpuLim != "" {
		lim[corev1.ResourceCPU] = resource.MustParse(r.cpuLim)
	}
	if r.memLim != "" {
		lim[corev1.ResourceMemory] = resource.MustParse(r.memLim)
	}
	return corev1.ResourceRequirements{Requests: req, Limits: lim}
}

// deployHamster creates a Deployment of `replicas` busybox pods at the given
// resources, with resizePolicy NotRequired so resizes are live (no restart),
// and waits until all pods are Running.
func deployHamster(ctx context.Context, ns string, replicas int32, r resources) *appsv1.Deployment {
	labels := map[string]string{"app": hamsterCtr}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: hamsterCtr, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(1)),
					Containers: []corev1.Container{{
						Name:      hamsterCtr,
						Image:     hamsterImage,
						Command:   []string{"/bin/sh", "-c", hamsterCommand},
						Resources: r.requirements(),
						ResizePolicy: []corev1.ContainerResizePolicy{
							{ResourceName: corev1.ResourceCPU, RestartPolicy: corev1.NotRequired},
							{ResourceName: corev1.ResourceMemory, RestartPolicy: corev1.NotRequired},
						},
					}},
				},
			},
		},
	}
	_, err := env.clientset.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	waitDeploymentReady(ctx, ns, hamsterCtr, replicas)
	return dep
}

func waitDeploymentReady(ctx context.Context, ns, name string, want int32) {
	gomega.Eventually(func(g gomega.Gomega) {
		d, err := env.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(d.Status.ReadyReplicas).To(gomega.Equal(want))
	}, 2*time.Minute, 3*time.Second).Should(gomega.Succeed())
}

func hamsterPods(ctx context.Context, ns string) []corev1.Pod {
	pl, err := env.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + hamsterCtr,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pl.Items
}

func singleHamsterPod(ctx context.Context, ns string) corev1.Pod {
	pods := hamsterPods(ctx, ns)
	// During a rolling update the Deployment briefly has 2 pods:
	// old (terminating) and new. Return the first non-terminating one.
	for _, p := range pods {
		if p.DeletionTimestamp == nil {
			return p
		}
	}
	gomega.ExpectWithOffset(1, pods).To(gomega.HaveLen(1), "expected at least one non-terminating hamster pod")
	return pods[0]
}

// --- cpvpa controller + RBAC ------------------------------------------------

type cpvpaOpts struct {
	mode      string // Recreate | InPlace | InPlaceOrRecreate
	config    string // JSON scaling config (default-config)
	grace     string // e.g. "20s"; only for InPlaceOrRecreate
	maxPods   int    // only for InPlaceOrRecreate
	verbosity string // glog -v, default "1" so we can read resize-cycle lines
}

// deployCPVPA installs the RBAC the feature actually needs (this doubles as
// the RBAC end-to-end check) and a cpvpa Deployment pointed at the hamster.
func deployCPVPA(ctx context.Context, ns string, o cpvpaOpts) {
	sa := "cpvpa"
	mustCreate(env.clientset.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: sa, Namespace: ns},
	}, metav1.CreateOptions{}))

	cr := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "cpvpa-" + ns},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"list"}},
			// get is needed by getTargetSelector(); patch for the template.
			{APIGroups: []string{"apps", "extensions"},
				Resources: []string{"deployments", "daemonsets", "replicasets"},
				Verbs:     []string{"get", "patch"}},
			// the in-place paths:
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"list", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"pods/resize"}, Verbs: []string{"patch"}},
		},
	}
	mustCreate(env.clientset.RbacV1().ClusterRoles().Create(ctx, cr, metav1.CreateOptions{}))
	mustCreate(env.clientset.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "cpvpa-" + ns},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: sa, Namespace: ns}},
		RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: "cpvpa-" + ns, APIGroup: "rbac.authorization.k8s.io"},
	}, metav1.CreateOptions{}))

	args := []string{
		"/cpvpa",
		"--target=deployment/" + hamsterCtr,
		"--namespace=" + ns,
		"--logtostderr=true",
		"-v=" + def(o.verbosity, "1"),
		fmt.Sprintf("--poll-period-seconds=%d", pollPeriodSecs),
		"--default-config=" + o.config,
		"--resize-mode=" + o.mode,
	}
	if o.mode == "InPlaceOrRecreate" {
		args = append(args,
			"--resize-fallback-grace-period="+def(o.grace, "20s"),
			fmt.Sprintf("--resize-fallback-max-pods-per-cycle=%d", maxInt(o.maxPods, 1)))
	}

	labels := map[string]string{"app": "cpvpa"}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "cpvpa", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: sa,
					Containers: []corev1.Container{{
						Name:            "cpvpa",
						Image:           env.cpvpaImage,
						ImagePullPolicy: corev1.PullIfNotPresent, // image is kind-loaded
						Command:         args,
					}},
				},
			},
		},
	}
	mustCreate(env.clientset.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{}))
}

// cpvpaLogs returns the current logs of the cpvpa pod (for log-based asserts
// such as the partial-management no-churn check).
func cpvpaLogs(ctx context.Context, ns string) string {
	pl, err := env.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=cpvpa"})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pl.Items).NotTo(gomega.BeEmpty())
	req := env.clientset.CoreV1().Pods(ns).GetLogs(pl.Items[0].Name, &corev1.PodLogOptions{})
	rc, err := req.Stream(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer rc.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(rc)
	return buf.String()
}

// --- resize assertions ------------------------------------------------------

// appliedCPUMilli reads the kubelet-reported *actual* CPU request from
// pod.Status (status.containerStatuses[].resources), which is the source of
// truth for what's been applied (populated >=1.33).
func appliedCPUReqMilli(pod *corev1.Pod) int64 {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == hamsterCtr && cs.Resources != nil {
			if q, ok := cs.Resources.Requests[corev1.ResourceCPU]; ok {
				return q.MilliValue()
			}
		}
	}
	return -1
}

func restartCount(pod *corev1.Pod) int32 {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == hamsterCtr {
			return cs.RestartCount
		}
	}
	return -1
}

func getPod(ctx context.Context, ns, name string) *corev1.Pod {
	p, err := env.clientset.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return p
}

// cpuMaxMilli execs into the pod and reads cgroup v2 cpu.max, returning the
// effective CPU *limit* in milli-cores. This is the strongest proof a resize
// actually reached the kernel, not just the API object.
func cpuMaxMilli(ctx context.Context, ns, pod string) int64 {
	out := execInPod(ctx, ns, pod, "cat", "/sys/fs/cgroup/cpu.max")
	fields := strings.Fields(strings.TrimSpace(out))
	gomega.Expect(fields).To(gomega.HaveLen(2), "unexpected cpu.max: %q", out)
	if fields[0] == "max" {
		return -1 // unlimited
	}
	quota, err := strconv.ParseInt(fields[0], 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	period, err := strconv.ParseInt(fields[1], 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return quota * 1000 / period
}

// memMaxBytes execs into the pod and reads cgroup v2 memory.max, the
// effective memory limit in bytes.
func memMaxBytes(ctx context.Context, ns, pod string) int64 {
	out := strings.TrimSpace(execInPod(ctx, ns, pod, "cat", "/sys/fs/cgroup/memory.max"))
	if out == "max" {
		return -1 // unlimited
	}
	v, err := strconv.ParseInt(out, 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "unexpected memory.max: %q", out)
	return v
}

func execInPod(ctx context.Context, ns, pod string, cmd ...string) string {
	req := env.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(pod).Namespace(ns).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: hamsterCtr,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(env.restConfig, "POST", req.URL())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "stderr: %s", stderr.String())
	return stdout.String()
}

// --- version gating ---------------------------------------------------------

func skipBelow(minor int, reason string) {
	if env.serverMinor < minor {
		ginkgo.Skip(fmt.Sprintf("requires k8s 1.%d+ (%s); cluster is 1.%d", minor, reason, env.serverMinor))
	}
}

func skipAtLeast(minor int, reason string) {
	if env.serverMinor >= minor {
		ginkgo.Skip(fmt.Sprintf("only meaningful below k8s 1.%d (%s); cluster is 1.%d", minor, reason, env.serverMinor))
	}
}

// --- tiny helpers -----------------------------------------------------------

func ptr[T any](v T) *T { return &v }

func def(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func maxInt(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}

func mustCreate(_ interface{}, err error) {
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}
