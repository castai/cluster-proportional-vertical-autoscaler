/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package e2e

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func TestE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "cpvpa in-place resize e2e")
}

var _ = ginkgo.BeforeSuite(func() {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = clientcmd.RecommendedHomeFile
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "need a reachable cluster via KUBECONFIG")

	cs, err := kubernetes.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ver, err := cs.Discovery().ServerVersion()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	minor, err := strconv.Atoi(strings.TrimRight(ver.Minor, "+"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "could not parse server minor %q", ver.Minor)

	image := os.Getenv("CPVPA_IMAGE")
	if image == "" {
		image = "cpvpa:e2e"
	}

	env = testEnv{
		clientset:   cs,
		restConfig:  cfg,
		cpvpaImage:  image,
		serverMinor: minor,
	}
	ginkgo.GinkgoWriter.Printf("e2e against k8s 1.%d, cpvpa image %q\n", minor, image)
})
