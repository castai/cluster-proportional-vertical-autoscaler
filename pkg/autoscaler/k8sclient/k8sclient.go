/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler/pkg/version"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/clock"
)

// K8sClient - Wraps all needed client functionalities for autoscaler
type K8sClient interface {
	// GetClusterSize counts schedulable nodes and cores in the cluster
	GetClusterSize(ctx context.Context) (*ClusterSize, error)
	// UpdateResources updates the resource needs for the containers in the target.
	// reqsChanged indicates the desired resources differ from what cpvpa last
	// applied; it gates the (disruptive) template patch. When resize mode is
	// InPlace or InPlaceOrRecreate, running pods are converged via the /resize
	// subresource and the template is left untouched on the happy path.
	UpdateResources(ctx context.Context, resources map[string]v1.ResourceRequirements, reqsChanged bool) error
}

// k8sClient - Wraps all Kubernetes API client functionality.
type k8sClient struct {
	clientset  kubernetes.Interface
	resizeMode ResizeMode
	target     *targetClient
	podResizer *podResizer
}

// NewK8sClient gives a k8sClient with the given dependencies.
func NewK8sClient(namespace, target, kubeconfig string, dryRun bool, mode ResizeMode, fallbackCfg ResizeFallbackConfig, clk clock.PassiveClock) (K8sClient, error) {
	var config *rest.Config
	var err error
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	config.UserAgent = userAgent()
	// Use protobufs for communication with apiserver.
	config.ContentType = "application/vnd.kubernetes.protobuf"
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	tgt, err := makeTarget(clientset, target, namespace)
	if err != nil {
		return nil, err
	}
	tc := newTargetClient(*tgt, clientset, dryRun)

	if mode == ResizeModeInPlace {
		if err := EnsureResizeSubresource(clientset); err != nil {
			return nil, fmt.Errorf("in-place resize requires the pods/resize subresource: %w", err)
		}
	} else if mode == ResizeModeInPlaceOrRecreate {
		if err := EnsureResizeSubresource(clientset); err != nil {
			glog.Warningf("pods/resize unavailable (%v); %s degrading to %s", err, mode, ResizeModeRecreate)
			mode = ResizeModeRecreate
		}
	}

	var resizer *podResizer
	if mode != ResizeModeRecreate {
		resizer = &podResizer{
			resizeMode:     mode,
			fallbackConfig: fallbackCfg,
			dryRun:         dryRun,
			clock:          clk,
			clientset:      clientset,
			target:         tc,
			tracker:        newResizeTracker(),
		}
	}

	return newK8sClient(clientset, tc, resizer, mode)
}

// newK8sClient builds a k8sClient from its core dependencies.
func newK8sClient(
	clientset kubernetes.Interface,
	target *targetClient,
	podResizer *podResizer,
	mode ResizeMode,
) (*k8sClient, error) {
	return &k8sClient{
		clientset:  clientset,
		target:     target,
		podResizer: podResizer,
		resizeMode: mode,
	}, nil
}

func userAgent() string {
	command := ""
	if len(os.Args) > 0 && len(os.Args[0]) > 0 {
		command = filepath.Base(os.Args[0])
	}
	if len(command) == 0 {
		command = "cpvpa"
	}
	return command + "/" + version.Version
}

func makeTarget(client kubernetes.Interface, target, namespace string) (*targetSpec, error) {
	splits := strings.Split(target, "/")
	if len(splits) != 2 {
		return nil, fmt.Errorf("target format error: %v", target)
	}
	kind := splits[0]
	name := splits[1]

	kind, groupVersions, err := discoverAPI(client, kind)
	if err != nil {
		return nil, err
	}

	tgt, err := newTargetSpec(kind, groupVersions, namespace, name)
	if err != nil {
		return nil, err
	}

	glog.V(4).Infof("Discovered target %s in %v", target, tgt.GroupVersion)
	return tgt, nil
}

func discoverAPI(client kubernetes.Interface, kindArg string) (kind string, groupVersions map[string]bool, err error) {
	var plural string
	switch strings.ToLower(kindArg) {
	case "deployment":
		kind = "Deployment"
		plural = "deployments"
	case "daemonset":
		kind = "DaemonSet"
		plural = "daemonsets"
	case "replicaset":
		kind = "ReplicaSet"
		plural = "replicasets"
	default:
		return "", nil, fmt.Errorf("unknown kind %q", kindArg)
	}

	resourceLists, err := client.Discovery().ServerPreferredNamespacedResources()
	if err != nil {
		if !discovery.IsGroupDiscoveryFailedError(err) {
			return "", nil, fmt.Errorf("failed to discover preferred resources: %v", err)
		}
		glog.Warningf("Failed to discover some groups: %v", err)
	}

	groupVersions = map[string]bool{}
	for _, resourceList := range resourceLists {
		for _, res := range resourceList.APIResources {
			if res.Name == plural && res.Kind == kind {
				groupVersions[resourceList.GroupVersion] = true
			}
		}
	}

	if len(groupVersions) == 0 {
		return "", nil, fmt.Errorf("failed to discover apigroup for kind %q", kind)
	}

	return kind, groupVersions, nil
}

// ClusterSize defines the cluster status.
type ClusterSize struct {
	Nodes int
	Cores int
}

func (k *k8sClient) GetClusterSize(ctx context.Context) (clusterStatus *ClusterSize, err error) {
	opt := metav1.ListOptions{Watch: false}

	nodes, err := k.clientset.CoreV1().Nodes().List(ctx, opt)
	if err != nil || nodes == nil {
		return nil, err
	}
	clusterStatus = &ClusterSize{}
	clusterStatus.Nodes = len(nodes.Items)
	var tc resource.Quantity
	// All nodes are considered, even those that are marked as unshedulable,
	// this includes the master.
	for _, node := range nodes.Items {
		tc.Add(node.Status.Capacity[v1.ResourceCPU])
	}

	tcInt64, tcOk := tc.AsInt64()
	if !tcOk {
		return nil, fmt.Errorf("unable to compute integer values of cores in the cluster")
	}
	clusterStatus.Cores = int(tcInt64)
	return clusterStatus, nil
}

func (k *k8sClient) UpdateResources(ctx context.Context, resources map[string]v1.ResourceRequirements, reqsChanged bool) error {
	if k.resizeMode == ResizeModeRecreate {
		if !reqsChanged {
			return nil
		}
		return k.target.PatchTemplate(ctx, resources)
	}

	result, err := k.podResizer.resizeRunningPods(ctx, resources)
	glog.V(1).Infof("resize cycle: %+v", result)
	if err != nil {
		return fmt.Errorf("in-place resize: %w", err)
	}
	return nil
}
