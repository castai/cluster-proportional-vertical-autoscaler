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

package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	realk8sclient "github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler/pkg/autoscaler/k8sclient"
	k8sclient "github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler/pkg/autoscaler/k8sclient/testing"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestRun(t *testing.T) {
	var asConfig = `
{
  "fake-agent": {
    "requests": {
      "cpu": {
        "base": "10m", "step":"1m", "coresPerStep":1
      },
      "memory": {
        "base": "8Mi", "step":"1Mi", "coresPerStep":1
      }
    }
  }
}
`
	mockK8s := k8sclient.MockK8sClient{
		NumOfNodes: 4,
		NumOfCores: 7,
	}

	fakeClock := clocktesting.NewFakeClock(time.Now())
	fakePollPeriod := 5 * time.Second
	cfg := ScaleConfig{}
	if err := json.Unmarshal([]byte(asConfig), &cfg); err != nil {
		log.Fatalf("invalid default config: %v", err)
	}
	autoScaler := &AutoScaler{
		k8sClient:     &mockK8s,
		defaultConfig: cfg,
		configFile:    asConfig,
		pollPeriod:    fakePollPeriod,
		clock:         fakeClock,
		stopCh:        make(chan struct{}),
		readyCh:       make(chan<- struct{}, 1),
	}

	go autoScaler.Run()
	defer close(autoScaler.stopCh)
}

func TestCalculatePerCores(t *testing.T) {
	var coresPerStep = `
{
  "fake-agent": {
    "requests": {
      "cpu": {
        "base": "%dm", "step":"%dm", "coresPerStep":%d
      }
    }
  }
}
`
	for _, tt := range []struct {
		name     string
		numNodes int
		numCores int
		expVal   int64
		base     int
		step     int
		perStep  int
	}{
		{
			"base 10, step 1,  per step 1",
			4,
			7,
			17,
			10,
			1,
			1,
		},
		{
			"base 10, step 2, per step 1",
			4,
			7,
			24,
			10,
			2,
			1,
		},
		{
			"base 10, step 2, per step 2",
			4,
			20,
			30,
			10,
			2,
			2,
		},
		{
			"base 10, step 4, per step 3",
			4,
			20,
			20,
			10,
			1,
			2,
		},
		{
			"base 10, step 1, per step 0",
			4,
			20,
			10,
			10,
			1,
			0,
		},
		{
			"base 10, step 1, per step -22",
			4,
			20,
			10,
			10,
			1,
			-2,
		},
	} {
		mockK8s := k8sclient.MockK8sClient{
			NumOfNodes: tt.numNodes,
			NumOfCores: tt.numCores,
		}
		conf := fmt.Sprintf(coresPerStep, tt.base, tt.step, tt.perStep)
		cfg := ScaleConfig{}
		if err := json.Unmarshal([]byte(conf), &cfg); err != nil {
			t.Fatalf("invalid default config: %v", err)
		}

		sz, err := mockK8s.GetClusterSize(context.Background())
		if err != nil {
			t.Errorf("failed to get cluster size")
		}
		val := calculate(cfg["fake-agent"].Requests["cpu"], sz)
		if val != tt.expVal {
			t.Errorf("expected %d got %d", tt.expVal, val)
		}
	}
}

func TestCalculatePerNodes(t *testing.T) {
	var nodesPerStep = `
{
  "fake-agent": {
    "requests": {
      "cpu": {
        "base": "%dm", "step":"%dm", "nodesPerStep":%d
      }
    }
  }
}
`
	for _, tt := range []struct {
		name     string
		numNodes int
		numCores int
		expVal   int64
		base     int
		step     int
		perStep  int
	}{
		{
			"base 10, step 1,  per step 1",
			4,
			7,
			14,
			10,
			1,
			1,
		},
		{
			"base 10, step 2, per step 1",
			4,
			7,
			18,
			10,
			2,
			1,
		},
		{
			"base 10, step 2, per step 2",
			4,
			20,
			14,
			10,
			2,
			2,
		},
		{
			"base 10, step 4, per step 3",
			4,
			20,
			12,
			10,
			1,
			2,
		},
		{
			"base 10, step 1, per step 0",
			4,
			20,
			10,
			10,
			1,
			0,
		},
		{
			"base 10, step 1, per step -2",
			4,
			20,
			10,
			10,
			1,
			-2,
		},
	} {
		mockK8s := k8sclient.MockK8sClient{
			NumOfNodes: tt.numNodes,
			NumOfCores: tt.numCores,
		}
		conf := fmt.Sprintf(nodesPerStep, tt.base, tt.step, tt.perStep)
		cfg := ScaleConfig{}
		if err := json.Unmarshal([]byte(conf), &cfg); err != nil {
			t.Fatalf("invalid default config: %v", err)
		}

		sz, err := mockK8s.GetClusterSize(context.Background())
		if err != nil {
			t.Errorf("failed to get cluster size")
		}
		val := calculate(cfg["fake-agent"].Requests["cpu"], sz)
		if val != tt.expVal {
			t.Errorf("expected %d got %d", tt.expVal, val)
		}
	}
}

// recordingK8sClient is a K8sClient whose cluster size can change between
// polls and which records every UpdateResources call.
type recordingK8sClient struct {
	nodes int
	calls []recordedUpdate
}

type recordedUpdate struct {
	cpuRequest string
	changed    bool
}

func (c *recordingK8sClient) GetClusterSize(ctx context.Context) (*realk8sclient.ClusterSize, error) {
	return &realk8sclient.ClusterSize{Nodes: c.nodes, Cores: c.nodes}, nil
}

func (c *recordingK8sClient) UpdateResources(ctx context.Context, resources map[string]apiv1.ResourceRequirements, reqsChanged bool) error {
	q := resources["main"].Requests[apiv1.ResourceCPU]
	c.calls = append(c.calls, recordedUpdate{cpuRequest: q.String(), changed: reqsChanged})
	return nil
}

// TestPollAPIServer_ComputedRequestAboveLimit verifies that a config whose
// computed request outgrows its computed limit is never applied: the last
// valid resources are kept (and still passed on, so in-place modes keep
// converging), nothing is applied before a valid value exists, and the
// update resumes once the computed values are valid again.
func TestPollAPIServer_ComputedRequestAboveLimit(t *testing.T) {
	// request: 100m + 100m per node; limit: fixed 300m.
	const cfgJSON = `{"main":{
		"requests":{"cpu":{"base":"100m","step":"100m","nodesPerStep":1}},
		"limits":{"cpu":{"base":"300m"}}}}`
	cfg := ScaleConfig{}
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		t.Fatal(err)
	}
	client := &recordingK8sClient{}
	as := &AutoScaler{k8sClient: client, defaultConfig: cfg, pollPeriod: 5 * time.Second}
	poll := func(nodes int) {
		t.Helper()
		client.nodes = nodes
		if err := as.pollAPIServer(context.Background()); err != nil {
			t.Fatalf("nodes=%d: %v", nodes, err)
		}
	}

	poll(3) // 400m > 300m on the very first poll: nothing valid to apply
	if len(client.calls) != 0 {
		t.Fatalf("first poll invalid: UpdateResources called %v, want no call", client.calls)
	}
	poll(1) // 200m: valid
	poll(3) // 400m: invalid, keep 200m
	poll(2) // 300m: valid again
	want := []recordedUpdate{{"200m", true}, {"200m", false}, {"300m", true}}
	if fmt.Sprint(client.calls) != fmt.Sprint(want) {
		t.Errorf("UpdateResources calls = %v, want %v", client.calls, want)
	}
}

func TestComputedRequestAboveLimit(t *testing.T) {
	rr := func(req, lim string) apiv1.ResourceRequirements {
		r := apiv1.ResourceRequirements{Requests: apiv1.ResourceList{}, Limits: apiv1.ResourceList{}}
		if req != "" {
			r.Requests[apiv1.ResourceCPU] = resource.MustParse(req)
		}
		if lim != "" {
			r.Limits[apiv1.ResourceCPU] = resource.MustParse(lim)
		}
		return r
	}
	for _, tc := range []struct {
		name    string
		reqs    map[string]apiv1.ResourceRequirements
		wantBad bool
	}{
		{"below", map[string]apiv1.ResourceRequirements{"a": rr("100m", "200m")}, false},
		{"equal", map[string]apiv1.ResourceRequirements{"a": rr("200m", "200m")}, false},
		{"no limit", map[string]apiv1.ResourceRequirements{"a": rr("5", "")}, false},
		{"limit only", map[string]apiv1.ResourceRequirements{"a": rr("", "1m")}, false},
		{"above", map[string]apiv1.ResourceRequirements{"a": rr("100m", "200m"), "b": rr("300m", "200m")}, true},
	} {
		if got := computedRequestAboveLimit(tc.reqs); (got != "") != tc.wantBad {
			t.Errorf("%s: got %q, want violation=%v", tc.name, got, tc.wantBad)
		}
	}
}
